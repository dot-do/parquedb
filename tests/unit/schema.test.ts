import { describe, it, expect } from 'vitest'
import { parseFieldType, parseRelation, isRelationString } from '../../src/types/schema'

describe('Schema Parsing', () => {
  describe('parseFieldType', () => {
    it('parses basic types', () => {
      expect(parseFieldType('string')).toEqual({
        type: 'string',
        required: false,
        isArray: false,
        index: undefined,
        default: undefined,
      })
    })

    it('parses required modifier (!)', () => {
      expect(parseFieldType('string!')).toEqual({
        type: 'string',
        required: true,
        isArray: false,
        index: undefined,
        default: undefined,
      })
    })

    it('parses optional modifier (?)', () => {
      expect(parseFieldType('string?')).toEqual({
        type: 'string',
        required: false,
        isArray: false,
        index: undefined,
        default: undefined,
      })
    })

    it('parses array modifier ([])', () => {
      expect(parseFieldType('string[]')).toEqual({
        type: 'string',
        required: false,
        isArray: true,
        index: undefined,
        default: undefined,
      })
    })

    it('parses required array ([]!)', () => {
      expect(parseFieldType('string[]!')).toEqual({
        type: 'string',
        required: true,
        isArray: true,
        index: undefined,
        default: undefined,
      })
    })

    // Index modifiers (#)
    it('parses index modifier (#)', () => {
      expect(parseFieldType('string#')).toEqual({
        type: 'string',
        required: false,
        isArray: false,
        index: true,
        default: undefined,
      })
    })

    it('parses indexed + required (#!)', () => {
      expect(parseFieldType('string#!')).toEqual({
        type: 'string',
        required: true,
        isArray: false,
        index: true,
        default: undefined,
      })
    })

    it('parses unique index (##)', () => {
      expect(parseFieldType('email##')).toEqual({
        type: 'email',
        required: false,
        isArray: false,
        index: 'unique',
        default: undefined,
      })
    })

    it('parses unique + required (##!)', () => {
      expect(parseFieldType('email##!')).toEqual({
        type: 'email',
        required: true,
        isArray: false,
        index: 'unique',
        default: undefined,
      })
    })

    it('parses FTS index (#fts)', () => {
      expect(parseFieldType('text#fts')).toEqual({
        type: 'text',
        required: false,
        isArray: false,
        index: 'fts',
        default: undefined,
      })
    })

    it('parses vector index (#vec)', () => {
      expect(parseFieldType('vector(1536)#vec')).toEqual({
        type: 'vector(1536)',
        required: false,
        isArray: false,
        index: 'vector',
        default: undefined,
      })
    })

    it('parses hash index (#hash)', () => {
      expect(parseFieldType('string#hash')).toEqual({
        type: 'string',
        required: false,
        isArray: false,
        index: 'hash',
        default: undefined,
      })
    })

    it('parses parametric types with index', () => {
      expect(parseFieldType('decimal(10,2)#')).toEqual({
        type: 'decimal(10,2)',
        required: false,
        isArray: false,
        index: true,
        default: undefined,
      })
    })

    it('parses default values', () => {
      expect(parseFieldType('string = "draft"')).toEqual({
        type: 'string',
        required: false,
        isArray: false,
        index: undefined,
        default: '"draft"',
      })
    })
  })

  describe('parseRelation', () => {
    it('parses forward relation', () => {
      const result = parseRelation('-> User.posts')
      expect(result).toMatchObject({
        toType: 'User',
        reverse: 'posts',
        isArray: false,
        direction: 'forward',
        mode: 'exact',
      })
    })

    it('parses forward relation array', () => {
      const result = parseRelation('-> Category.posts[]')
      expect(result).toMatchObject({
        toType: 'Category',
        reverse: 'posts',
        isArray: true,
        direction: 'forward',
        mode: 'exact',
      })
    })

    it('parses backward relation', () => {
      const result = parseRelation('<- Comment.post')
      expect(result).toMatchObject({
        fromType: 'Comment',
        fromField: 'post',
        isArray: false,
        direction: 'backward',
        mode: 'exact',
      })
    })

    it('parses fuzzy forward relation', () => {
      const result = parseRelation('~> Topic')
      expect(result).toMatchObject({
        toType: 'Topic',
        direction: 'forward',
        mode: 'fuzzy',
      })
    })
  })

  describe('isRelationString', () => {
    it('identifies forward relations', () => {
      expect(isRelationString('-> User.posts')).toBe(true)
    })

    it('identifies backward relations', () => {
      expect(isRelationString('<- Comment.post')).toBe(true)
    })

    it('identifies fuzzy relations', () => {
      expect(isRelationString('~> Topic')).toBe(true)
      expect(isRelationString('<~ Source')).toBe(true)
    })

    it('rejects non-relations', () => {
      expect(isRelationString('string!')).toBe(false)
      expect(isRelationString('User')).toBe(false)
    })
  })
})
