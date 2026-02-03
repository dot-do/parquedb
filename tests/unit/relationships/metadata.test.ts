/**
 * Relationship Metadata Tests
 *
 * Tests for relationship metadata types with Variant shredding support.
 * Validates:
 * - RelationshipMetadata type functionality
 * - Shredded field extraction and merging
 * - Validation functions for matchMode and similarity
 * - Schema generation with shredded columns
 */

import { describe, it, expect } from 'vitest'
import {
  type RelationshipMetadata,
  type ShreddedRelationshipFields,
  type MatchMode,
  isMatchMode,
  isValidSimilarity,
  extractShreddedFields,
  mergeShreddedFields,
  validateSimilarity,
  validateMatchModeConsistency,
} from '../../../src/relationships/types'
import { createRelationshipSchema } from '../../../src/parquet/schema'

// =============================================================================
// Type Guard Tests
// =============================================================================

describe('isMatchMode', () => {
  it('should return true for "exact"', () => {
    expect(isMatchMode('exact')).toBe(true)
  })

  it('should return true for "fuzzy"', () => {
    expect(isMatchMode('fuzzy')).toBe(true)
  })

  it('should return false for invalid strings', () => {
    expect(isMatchMode('invalid')).toBe(false)
    expect(isMatchMode('Exact')).toBe(false)
    expect(isMatchMode('FUZZY')).toBe(false)
    expect(isMatchMode('')).toBe(false)
  })

  it('should return false for non-strings', () => {
    expect(isMatchMode(null)).toBe(false)
    expect(isMatchMode(undefined)).toBe(false)
    expect(isMatchMode(1)).toBe(false)
    expect(isMatchMode({})).toBe(false)
  })
})

describe('isValidSimilarity', () => {
  it('should return true for valid similarity scores', () => {
    expect(isValidSimilarity(0)).toBe(true)
    expect(isValidSimilarity(0.5)).toBe(true)
    expect(isValidSimilarity(1)).toBe(true)
    expect(isValidSimilarity(0.999)).toBe(true)
  })

  it('should return false for out-of-range numbers', () => {
    expect(isValidSimilarity(-0.1)).toBe(false)
    expect(isValidSimilarity(1.1)).toBe(false)
    expect(isValidSimilarity(-1)).toBe(false)
    expect(isValidSimilarity(100)).toBe(false)
  })

  it('should return false for non-numbers', () => {
    expect(isValidSimilarity('0.5')).toBe(false)
    expect(isValidSimilarity(null)).toBe(false)
    expect(isValidSimilarity(undefined)).toBe(false)
    expect(isValidSimilarity({})).toBe(false)
  })
})

// =============================================================================
// Validation Tests
// =============================================================================

describe('validateSimilarity', () => {
  it('should not throw for valid similarity scores', () => {
    expect(() => validateSimilarity(0)).not.toThrow()
    expect(() => validateSimilarity(0.5)).not.toThrow()
    expect(() => validateSimilarity(1)).not.toThrow()
  })

  it('should throw for negative similarity', () => {
    expect(() => validateSimilarity(-0.1)).toThrow('Similarity must be between 0 and 1')
  })

  it('should throw for similarity greater than 1', () => {
    expect(() => validateSimilarity(1.1)).toThrow('Similarity must be between 0 and 1')
  })
})

describe('validateMatchModeConsistency', () => {
  it('should not throw for valid exact match with no similarity', () => {
    expect(() => validateMatchModeConsistency('exact', undefined)).not.toThrow()
  })

  it('should not throw for valid exact match with similarity 1.0', () => {
    expect(() => validateMatchModeConsistency('exact', 1.0)).not.toThrow()
  })

  it('should throw for exact match with non-1.0 similarity', () => {
    expect(() => validateMatchModeConsistency('exact', 0.8)).toThrow(
      'Exact match mode should have similarity of 1.0 or undefined'
    )
  })

  it('should not throw for valid fuzzy match with similarity', () => {
    expect(() => validateMatchModeConsistency('fuzzy', 0.85)).not.toThrow()
    expect(() => validateMatchModeConsistency('fuzzy', 0.5)).not.toThrow()
  })

  it('should throw for fuzzy match without similarity', () => {
    expect(() => validateMatchModeConsistency('fuzzy', undefined)).toThrow(
      'Fuzzy match mode requires a similarity score'
    )
  })

  it('should not throw when matchMode is undefined', () => {
    expect(() => validateMatchModeConsistency(undefined, 0.5)).not.toThrow()
    expect(() => validateMatchModeConsistency(undefined, undefined)).not.toThrow()
  })
})

// =============================================================================
// Shredded Field Extraction and Merging Tests
// =============================================================================

describe('extractShreddedFields', () => {
  it('should extract matchMode from metadata', () => {
    const metadata: RelationshipMetadata = {
      matchMode: 'exact',
      source: 'user-input',
    }

    const result = extractShreddedFields(metadata)

    expect(result.shredded).toEqual({ matchMode: 'exact' })
    expect(result.data).toEqual({ source: 'user-input' })
  })

  it('should extract similarity from metadata', () => {
    const metadata: RelationshipMetadata = {
      similarity: 0.85,
      matchedType: 'name-match',
    }

    const result = extractShreddedFields(metadata)

    expect(result.shredded).toEqual({ similarity: 0.85 })
    expect(result.data).toEqual({ matchedType: 'name-match' })
  })

  it('should extract both matchMode and similarity', () => {
    const metadata: RelationshipMetadata = {
      matchMode: 'fuzzy',
      similarity: 0.92,
      matchedType: 'embedding-match',
      confidence: 'high',
    }

    const result = extractShreddedFields(metadata)

    expect(result.shredded).toEqual({
      matchMode: 'fuzzy',
      similarity: 0.92,
    })
    expect(result.data).toEqual({
      matchedType: 'embedding-match',
      confidence: 'high',
    })
  })

  it('should handle empty metadata', () => {
    const metadata: RelationshipMetadata = {}

    const result = extractShreddedFields(metadata)

    expect(result.shredded).toEqual({})
    expect(result.data).toEqual({})
  })

  it('should handle metadata with only non-shredded fields', () => {
    const metadata: RelationshipMetadata = {
      source: 'import',
      confidence: 'medium',
      notes: 'Imported from external system',
    }

    const result = extractShreddedFields(metadata)

    expect(result.shredded).toEqual({})
    expect(result.data).toEqual({
      source: 'import',
      confidence: 'medium',
      notes: 'Imported from external system',
    })
  })

  it('should handle metadata with only shredded fields', () => {
    const metadata: RelationshipMetadata = {
      matchMode: 'fuzzy',
      similarity: 0.75,
    }

    const result = extractShreddedFields(metadata)

    expect(result.shredded).toEqual({
      matchMode: 'fuzzy',
      similarity: 0.75,
    })
    expect(result.data).toEqual({})
  })
})

describe('mergeShreddedFields', () => {
  it('should merge shredded fields into empty data', () => {
    const shredded: ShreddedRelationshipFields = {
      matchMode: 'exact',
      similarity: 1.0,
    }

    const result = mergeShreddedFields(shredded, undefined)

    expect(result).toEqual({
      matchMode: 'exact',
      similarity: 1.0,
    })
  })

  it('should merge shredded fields with existing data', () => {
    const shredded: ShreddedRelationshipFields = {
      matchMode: 'fuzzy',
      similarity: 0.88,
    }
    const data: Record<string, unknown> = {
      matchedType: 'vector-similarity',
      source: 'ml-model',
    }

    const result = mergeShreddedFields(shredded, data)

    expect(result).toEqual({
      matchMode: 'fuzzy',
      similarity: 0.88,
      matchedType: 'vector-similarity',
      source: 'ml-model',
    })
  })

  it('should handle empty shredded fields', () => {
    const shredded: ShreddedRelationshipFields = {}
    const data: Record<string, unknown> = {
      source: 'manual',
    }

    const result = mergeShreddedFields(shredded, data)

    expect(result).toEqual({
      source: 'manual',
    })
  })

  it('should handle partial shredded fields', () => {
    const shredded: ShreddedRelationshipFields = {
      matchMode: 'exact',
    }
    const data: Record<string, unknown> = {
      notes: 'User confirmed',
    }

    const result = mergeShreddedFields(shredded, data)

    expect(result).toEqual({
      matchMode: 'exact',
      notes: 'User confirmed',
    })
  })

  it('should be inverse of extractShreddedFields', () => {
    const original: RelationshipMetadata = {
      matchMode: 'fuzzy',
      similarity: 0.95,
      matchedType: 'semantic',
      confidence: 'high',
      source: 'entity-resolution',
    }

    const { shredded, data } = extractShreddedFields(original)
    const reconstructed = mergeShreddedFields(shredded, data)

    expect(reconstructed).toEqual(original)
  })
})

// =============================================================================
// Roundtrip Tests
// =============================================================================

describe('shredding roundtrip', () => {
  it('should preserve all data through extract/merge cycle', () => {
    const testCases: RelationshipMetadata[] = [
      // Case 1: Full metadata
      {
        matchMode: 'fuzzy',
        similarity: 0.85,
        matchedType: 'name-match',
        confidence: 'high',
        source: 'entity-resolution-service',
      },
      // Case 2: Exact match only
      {
        matchMode: 'exact',
      },
      // Case 3: Fuzzy with minimal data
      {
        matchMode: 'fuzzy',
        similarity: 0.5,
      },
      // Case 4: No shredded fields
      {
        source: 'manual',
        notes: 'User created',
      },
      // Case 5: Empty
      {},
    ]

    for (const original of testCases) {
      const { shredded, data } = extractShreddedFields(original)
      const reconstructed = mergeShreddedFields(shredded, data)
      expect(reconstructed).toEqual(original)
    }
  })
})

// =============================================================================
// Schema Tests
// =============================================================================

describe('createRelationshipSchema', () => {
  it('should include matchMode as a shredded column', () => {
    const schema = createRelationshipSchema()

    expect(schema.matchMode).toBeDefined()
    expect(schema.matchMode.type).toBe('STRING')
    expect(schema.matchMode.optional).toBe(true)
  })

  it('should include similarity as a shredded column', () => {
    const schema = createRelationshipSchema()

    expect(schema.similarity).toBeDefined()
    expect(schema.similarity.type).toBe('DOUBLE')
    expect(schema.similarity.optional).toBe(true)
  })

  it('should include data column for remaining Variant data', () => {
    const schema = createRelationshipSchema()

    expect(schema.data).toBeDefined()
    expect(schema.data.type).toBe('BYTE_ARRAY')
    expect(schema.data.optional).toBe(true)
  })

  it('should include all required relationship columns', () => {
    const schema = createRelationshipSchema()

    // Source entity columns
    expect(schema.fromNs).toBeDefined()
    expect(schema.fromId).toBeDefined()
    expect(schema.fromType).toBeDefined()
    expect(schema.fromName).toBeDefined()

    // Relationship name columns
    expect(schema.predicate).toBeDefined()
    expect(schema.reverse).toBeDefined()

    // Target entity columns
    expect(schema.toNs).toBeDefined()
    expect(schema.toId).toBeDefined()
    expect(schema.toType).toBeDefined()
    expect(schema.toName).toBeDefined()

    // Audit columns
    expect(schema.createdAt).toBeDefined()
    expect(schema.createdBy).toBeDefined()
    expect(schema.deletedAt).toBeDefined()
    expect(schema.deletedBy).toBeDefined()
    expect(schema.version).toBeDefined()
  })
})
