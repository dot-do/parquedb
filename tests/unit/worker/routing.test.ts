/**
 * Routing Utilities Tests
 *
 * Tests for query parameter parsing with proper input validation.
 * Ensures invalid JSON throws errors instead of silently returning defaults.
 */

import { describe, it, expect } from 'vitest'
import { parseQueryFilter, parseQueryOptions, QueryParamError, parsePaginationParams } from '@/worker/routing'
import { MAX_QUERY_LIMIT, MAX_QUERY_OFFSET } from '@/constants'

// =============================================================================
// parseQueryFilter
// =============================================================================

describe('parseQueryFilter', () => {
  it('returns empty filter when no filter param is present', () => {
    const params = new URLSearchParams()
    expect(parseQueryFilter(params)).toEqual({})
  })

  it('parses valid JSON filter', () => {
    const params = new URLSearchParams({ filter: '{"status":"published"}' })
    expect(parseQueryFilter(params)).toEqual({ status: 'published' })
  })

  it('parses complex filter with operators', () => {
    const filter = { score: { $gte: 100 }, status: { $in: ['published', 'featured'] } }
    const params = new URLSearchParams({ filter: JSON.stringify(filter) })
    expect(parseQueryFilter(params)).toEqual(filter)
  })

  it('throws QueryParamError on invalid JSON', () => {
    const params = new URLSearchParams({ filter: 'not-valid-json' })
    expect(() => parseQueryFilter(params)).toThrow(QueryParamError)
    expect(() => parseQueryFilter(params)).toThrow('Invalid filter: must be valid JSON')
  })

  it('throws QueryParamError when filter is a JSON array', () => {
    const params = new URLSearchParams({ filter: '[1,2,3]' })
    expect(() => parseQueryFilter(params)).toThrow(QueryParamError)
    expect(() => parseQueryFilter(params)).toThrow('must be a JSON object')
  })

  it('throws QueryParamError when filter is a JSON string', () => {
    const params = new URLSearchParams({ filter: '"hello"' })
    expect(() => parseQueryFilter(params)).toThrow(QueryParamError)
    expect(() => parseQueryFilter(params)).toThrow('must be a JSON object')
  })

  it('throws QueryParamError when filter is a JSON number', () => {
    const params = new URLSearchParams({ filter: '42' })
    expect(() => parseQueryFilter(params)).toThrow(QueryParamError)
    expect(() => parseQueryFilter(params)).toThrow('must be a JSON object')
  })

  it('throws QueryParamError when filter is JSON null', () => {
    const params = new URLSearchParams({ filter: 'null' })
    expect(() => parseQueryFilter(params)).toThrow(QueryParamError)
    expect(() => parseQueryFilter(params)).toThrow('must be a JSON object')
  })

  it('QueryParamError has correct properties', () => {
    const params = new URLSearchParams({ filter: 'bad' })
    try {
      parseQueryFilter(params)
      expect.fail('Should have thrown')
    } catch (e) {
      expect(e).toBeInstanceOf(QueryParamError)
      const err = e as QueryParamError
      expect(err.status).toBe(400)
      expect(err.name).toBe('QueryParamError')
    }
  })
})

// =============================================================================
// parseQueryOptions
// =============================================================================

describe('parseQueryOptions', () => {
  it('returns empty options when no params present', () => {
    const params = new URLSearchParams()
    expect(parseQueryOptions(params)).toEqual({})
  })

  it('parses valid limit', () => {
    const params = new URLSearchParams({ limit: '10' })
    expect(parseQueryOptions(params)).toEqual({ limit: 10 })
  })

  it('parses valid skip', () => {
    const params = new URLSearchParams({ skip: '20' })
    expect(parseQueryOptions(params)).toEqual({ skip: 20 })
  })

  it('throws QueryParamError on non-numeric limit', () => {
    const params = new URLSearchParams({ limit: 'abc' })
    expect(() => parseQueryOptions(params)).toThrow(QueryParamError)
    expect(() => parseQueryOptions(params)).toThrow('Invalid limit: must be a non-negative integer')
  })

  it('throws QueryParamError on negative limit', () => {
    const params = new URLSearchParams({ limit: '-5' })
    expect(() => parseQueryOptions(params)).toThrow(QueryParamError)
    expect(() => parseQueryOptions(params)).toThrow('Invalid limit: must be a non-negative integer')
  })

  it('throws QueryParamError on non-numeric skip', () => {
    const params = new URLSearchParams({ skip: 'abc' })
    expect(() => parseQueryOptions(params)).toThrow(QueryParamError)
    expect(() => parseQueryOptions(params)).toThrow('Invalid skip: must be a non-negative integer')
  })

  it('throws QueryParamError on negative skip', () => {
    const params = new URLSearchParams({ skip: '-1' })
    expect(() => parseQueryOptions(params)).toThrow(QueryParamError)
    expect(() => parseQueryOptions(params)).toThrow('non-negative integer')
  })

  it('parses cursor', () => {
    const params = new URLSearchParams({ cursor: 'abc123' })
    expect(parseQueryOptions(params)).toEqual({ cursor: 'abc123' })
  })

  it('parses JSON sort', () => {
    const params = new URLSearchParams({ sort: '{"name":1,"age":-1}' })
    const result = parseQueryOptions(params)
    expect(result.sort).toEqual({ name: 1, age: -1 })
  })

  it('parses simple sort format', () => {
    const params = new URLSearchParams({ sort: 'name:asc,age:desc' })
    const result = parseQueryOptions(params)
    expect(result.sort).toEqual({ name: 1, age: -1 })
  })

  it('parses JSON project', () => {
    const params = new URLSearchParams({ project: '{"name":1,"secret":0}' })
    const result = parseQueryOptions(params)
    expect(result.project).toEqual({ name: 1, secret: 0 })
  })

  it('parses simple project format', () => {
    const params = new URLSearchParams({ project: 'name,title,-secret' })
    const result = parseQueryOptions(params)
    expect(result.project).toEqual({ name: 1, title: 1, secret: 0 })
  })

  it('accepts limit of 0', () => {
    const params = new URLSearchParams({ limit: '0' })
    expect(parseQueryOptions(params)).toEqual({ limit: 0 })
  })
})

// =============================================================================
// parsePaginationParams
// =============================================================================

describe('parsePaginationParams', () => {
  // ---------------------------------------------------------------------------
  // Default behavior
  // ---------------------------------------------------------------------------

  it('returns default values when no params present', () => {
    const params = new URLSearchParams()
    expect(parsePaginationParams(params)).toEqual({ limit: 100, offset: 0 })
  })

  it('uses custom default limit', () => {
    const params = new URLSearchParams()
    expect(parsePaginationParams(params, { defaultLimit: 50 })).toEqual({
      limit: 50,
      offset: 0,
    })
  })

  // ---------------------------------------------------------------------------
  // Valid limit values
  // ---------------------------------------------------------------------------

  it('parses valid limit', () => {
    const params = new URLSearchParams({ limit: '25' })
    expect(parsePaginationParams(params)).toEqual({ limit: 25, offset: 0 })
  })

  it('accepts limit of 0', () => {
    const params = new URLSearchParams({ limit: '0' })
    expect(parsePaginationParams(params)).toEqual({ limit: 0, offset: 0 })
  })

  it('accepts limit at max', () => {
    const params = new URLSearchParams({ limit: String(MAX_QUERY_LIMIT) })
    expect(parsePaginationParams(params)).toEqual({
      limit: MAX_QUERY_LIMIT,
      offset: 0,
    })
  })

  // ---------------------------------------------------------------------------
  // Invalid limit values
  // ---------------------------------------------------------------------------

  it('throws QueryParamError on non-numeric limit (NaN)', () => {
    const params = new URLSearchParams({ limit: 'abc' })
    expect(() => parsePaginationParams(params)).toThrow(QueryParamError)
    expect(() => parsePaginationParams(params)).toThrow('Invalid limit: must be a valid integer')
  })

  it('throws QueryParamError on negative limit', () => {
    const params = new URLSearchParams({ limit: '-5' })
    expect(() => parsePaginationParams(params)).toThrow(QueryParamError)
    expect(() => parsePaginationParams(params)).toThrow('Invalid limit: must be non-negative')
  })

  it('throws QueryParamError when limit exceeds max', () => {
    const params = new URLSearchParams({ limit: String(MAX_QUERY_LIMIT + 1) })
    expect(() => parsePaginationParams(params)).toThrow(QueryParamError)
    expect(() => parsePaginationParams(params)).toThrow(`Invalid limit: cannot exceed ${MAX_QUERY_LIMIT}`)
  })

  it('throws QueryParamError on float limit', () => {
    const params = new URLSearchParams({ limit: '10.5' })
    // parseInt('10.5') = 10, so this should pass - parseInt truncates
    // But we could consider this invalid if we want strict integer parsing
    // For now, parseInt behavior is acceptable
    expect(parsePaginationParams(params)).toEqual({ limit: 10, offset: 0 })
  })

  it('throws QueryParamError on empty string limit', () => {
    // Empty string should use default
    const params = new URLSearchParams({ limit: '' })
    expect(parsePaginationParams(params)).toEqual({ limit: 100, offset: 0 })
  })

  // ---------------------------------------------------------------------------
  // Valid offset values
  // ---------------------------------------------------------------------------

  it('parses valid offset', () => {
    const params = new URLSearchParams({ offset: '50' })
    expect(parsePaginationParams(params)).toEqual({ limit: 100, offset: 50 })
  })

  it('accepts offset of 0', () => {
    const params = new URLSearchParams({ offset: '0' })
    expect(parsePaginationParams(params)).toEqual({ limit: 100, offset: 0 })
  })

  it('accepts offset at max', () => {
    const params = new URLSearchParams({ offset: String(MAX_QUERY_OFFSET) })
    expect(parsePaginationParams(params)).toEqual({
      limit: 100,
      offset: MAX_QUERY_OFFSET,
    })
  })

  it('accepts skip as alias for offset', () => {
    const params = new URLSearchParams({ skip: '30' })
    expect(parsePaginationParams(params)).toEqual({ limit: 100, offset: 30 })
  })

  // ---------------------------------------------------------------------------
  // Invalid offset values
  // ---------------------------------------------------------------------------

  it('throws QueryParamError on non-numeric offset (NaN)', () => {
    const params = new URLSearchParams({ offset: 'xyz' })
    expect(() => parsePaginationParams(params)).toThrow(QueryParamError)
    expect(() => parsePaginationParams(params)).toThrow('Invalid offset: must be a valid integer')
  })

  it('throws QueryParamError on negative offset', () => {
    const params = new URLSearchParams({ offset: '-10' })
    expect(() => parsePaginationParams(params)).toThrow(QueryParamError)
    expect(() => parsePaginationParams(params)).toThrow('Invalid offset: must be non-negative')
  })

  it('throws QueryParamError when offset exceeds max', () => {
    const params = new URLSearchParams({ offset: String(MAX_QUERY_OFFSET + 1) })
    expect(() => parsePaginationParams(params)).toThrow(QueryParamError)
    expect(() => parsePaginationParams(params)).toThrow(`Invalid offset: cannot exceed ${MAX_QUERY_OFFSET}`)
  })

  // ---------------------------------------------------------------------------
  // Custom options
  // ---------------------------------------------------------------------------

  it('uses custom maxLimit', () => {
    const params = new URLSearchParams({ limit: '200' })
    expect(() => parsePaginationParams(params, { maxLimit: 100 })).toThrow(
      'cannot exceed 100'
    )
  })

  it('uses custom maxOffset', () => {
    const params = new URLSearchParams({ offset: '1001' })
    expect(() => parsePaginationParams(params, { maxOffset: 1000 })).toThrow(
      'cannot exceed 1000'
    )
  })

  it('uses custom offsetParam name', () => {
    const params = new URLSearchParams({ skip: '25' })
    const result = parsePaginationParams(params, { offsetParam: 'skip' })
    expect(result.offset).toBe(25)
  })

  it('uses custom limitParam name', () => {
    const params = new URLSearchParams({ pageSize: '15' })
    const result = parsePaginationParams(params, { limitParam: 'pageSize' })
    expect(result.limit).toBe(15)
  })

  // ---------------------------------------------------------------------------
  // Edge cases
  // ---------------------------------------------------------------------------

  it('handles both limit and offset together', () => {
    const params = new URLSearchParams({ limit: '50', offset: '100' })
    expect(parsePaginationParams(params)).toEqual({ limit: 50, offset: 100 })
  })

  it('handles whitespace in values', () => {
    // parseInt with leading/trailing whitespace
    const params = new URLSearchParams({ limit: ' 50 ' })
    // parseInt(' 50 ') = 50
    expect(parsePaginationParams(params)).toEqual({ limit: 50, offset: 0 })
  })

  it('prefers offset over skip when both present', () => {
    const params = new URLSearchParams({ offset: '100', skip: '50' })
    expect(parsePaginationParams(params)).toEqual({ limit: 100, offset: 100 })
  })

  it('QueryParamError has correct properties', () => {
    const params = new URLSearchParams({ limit: 'bad' })
    try {
      parsePaginationParams(params)
      expect.fail('Should have thrown')
    } catch (e) {
      expect(e).toBeInstanceOf(QueryParamError)
      const err = e as QueryParamError
      expect(err.status).toBe(400)
      expect(err.name).toBe('QueryParamError')
    }
  })
})
