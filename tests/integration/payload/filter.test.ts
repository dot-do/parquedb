/**
 * Tests for Payload CMS filter translation
 */

import { describe, it, expect } from 'vitest'
import {
  translatePayloadFilter,
  translatePayloadSort,
  convertLikeToRegex,
} from '../../../src/integrations/payload/filter'

describe('translatePayloadFilter', () => {
  describe('simple equality', () => {
    it('translates equals operator', () => {
      const result = translatePayloadFilter({
        status: { equals: 'published' },
      })
      expect(result).toEqual({ status: 'published' })
    })

    it('translates not_equals operator', () => {
      const result = translatePayloadFilter({
        status: { not_equals: 'draft' },
      })
      expect(result).toEqual({ status: { $ne: 'draft' } })
    })

    it('handles undefined/empty where', () => {
      expect(translatePayloadFilter(undefined)).toEqual({})
      expect(translatePayloadFilter({})).toEqual({})
    })
  })

  describe('comparison operators', () => {
    it('translates greater_than', () => {
      const result = translatePayloadFilter({
        views: { greater_than: 100 },
      })
      expect(result).toEqual({ views: { $gt: 100 } })
    })

    it('translates greater_than_equal', () => {
      const result = translatePayloadFilter({
        views: { greater_than_equal: 100 },
      })
      expect(result).toEqual({ views: { $gte: 100 } })
    })

    it('translates less_than', () => {
      const result = translatePayloadFilter({
        views: { less_than: 100 },
      })
      expect(result).toEqual({ views: { $lt: 100 } })
    })

    it('translates less_than_equal', () => {
      const result = translatePayloadFilter({
        views: { less_than_equal: 100 },
      })
      expect(result).toEqual({ views: { $lte: 100 } })
    })
  })

  describe('array operators', () => {
    it('translates in operator', () => {
      const result = translatePayloadFilter({
        status: { in: ['published', 'featured'] },
      })
      expect(result).toEqual({ status: { $in: ['published', 'featured'] } })
    })

    it('translates not_in operator', () => {
      const result = translatePayloadFilter({
        status: { not_in: ['draft', 'archived'] },
      })
      expect(result).toEqual({ status: { $nin: ['draft', 'archived'] } })
    })

    it('translates all operator', () => {
      const result = translatePayloadFilter({
        tags: { all: ['tech', 'news'] },
      })
      expect(result).toEqual({ tags: { $all: ['tech', 'news'] } })
    })
  })

  describe('existence operator', () => {
    it('translates exists: true', () => {
      const result = translatePayloadFilter({
        featuredImage: { exists: true },
      })
      expect(result).toEqual({ featuredImage: { $exists: true } })
    })

    it('translates exists: false', () => {
      const result = translatePayloadFilter({
        featuredImage: { exists: false },
      })
      expect(result).toEqual({ featuredImage: { $exists: false } })
    })
  })

  describe('string operators', () => {
    it('translates contains operator', () => {
      const result = translatePayloadFilter({
        title: { contains: 'hello' },
      })
      expect(result).toEqual({ title: { $contains: 'hello' } })
    })

    it('translates like operator to regex', () => {
      const result = translatePayloadFilter({
        title: { like: '%hello%' },
      })
      expect(result).toEqual({ title: { $regex: '.*hello.*' } })
    })

    it('translates not_like operator', () => {
      const result = translatePayloadFilter({
        title: { not_like: '%spam%' },
      })
      expect(result).toEqual({ title: { $not: { $regex: '.*spam.*' } } })
    })
  })

  describe('logical operators', () => {
    it('translates and operator', () => {
      const result = translatePayloadFilter({
        and: [
          { status: { equals: 'published' } },
          { featured: { equals: true } },
        ],
      })
      expect(result).toEqual({
        $and: [
          { status: 'published' },
          { featured: true },
        ],
      })
    })

    it('translates or operator', () => {
      const result = translatePayloadFilter({
        or: [
          { status: { equals: 'published' } },
          { status: { equals: 'featured' } },
        ],
      })
      expect(result).toEqual({
        $or: [
          { status: 'published' },
          { status: 'featured' },
        ],
      })
    })

    it('handles nested logical operators', () => {
      const result = translatePayloadFilter({
        and: [
          {
            or: [
              { status: { equals: 'published' } },
              { featured: { equals: true } },
            ],
          },
          { category: { equals: 'news' } },
        ],
      })
      expect(result).toEqual({
        $and: [
          {
            $or: [
              { status: 'published' },
              { featured: true },
            ],
          },
          { category: 'news' },
        ],
      })
    })
  })

  describe('combined filters', () => {
    it('handles multiple field conditions', () => {
      const result = translatePayloadFilter({
        status: { equals: 'published' },
        views: { greater_than: 100 },
        featured: { equals: true },
      })
      expect(result).toEqual({
        status: 'published',
        views: { $gt: 100 },
        featured: true,
      })
    })
  })
})

describe('translatePayloadSort', () => {
  it('handles single ascending field', () => {
    expect(translatePayloadSort('createdAt')).toEqual({ createdAt: 1 })
  })

  it('handles single descending field', () => {
    expect(translatePayloadSort('-createdAt')).toEqual({ createdAt: -1 })
  })

  it('handles array of fields', () => {
    expect(translatePayloadSort(['-createdAt', 'title'])).toEqual({
      createdAt: -1,
      title: 1,
    })
  })

  it('handles undefined', () => {
    expect(translatePayloadSort(undefined)).toBeUndefined()
  })
})

describe('convertLikeToRegex', () => {
  it('converts % to .*', () => {
    expect(convertLikeToRegex('%test%')).toBe('.*test.*')
  })

  it('converts _ to .', () => {
    expect(convertLikeToRegex('te_t')).toBe('^te.t$')
  })

  it('adds start anchor when no leading %', () => {
    expect(convertLikeToRegex('test%')).toBe('^test.*')
  })

  it('adds end anchor when no trailing %', () => {
    expect(convertLikeToRegex('%test')).toBe('.*test$')
  })

  it('escapes regex special characters', () => {
    expect(convertLikeToRegex('%test.com%')).toBe('.*test\\.com.*')
  })
})
