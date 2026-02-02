/**
 * Routing Utilities Tests
 *
 * Tests for query parameter parsing with proper input validation.
 * Ensures invalid JSON throws errors instead of silently returning defaults.
 */

import { describe, it, expect } from 'vitest'
import { parseQueryFilter, parseQueryOptions, QueryParamError } from '@/worker/routing'

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
