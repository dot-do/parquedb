/**
 * Relationship Metadata Shredding Tests
 *
 * Tests for relationship metadata with Variant shredding support.
 * Shredded fields (matchMode, similarity) are stored as top-level columns
 * for efficient predicate pushdown and filtering.
 *
 * @see /src/relationships/types.ts
 * @see /docs/architecture/variant-shredding.md
 */

import { describe, it, expect } from 'vitest'
import {
  type MatchMode,
  type RelationshipMetadata,
  type ShreddedRelationshipFields,
  type RelationshipFilter,
  type RelationshipWithMetadata,
  isMatchMode,
  isValidSimilarity,
  extractShreddedFields,
  mergeShreddedFields,
  validateSimilarity,
  validateMatchModeConsistency,
} from '../../../src/relationships/types'

// =============================================================================
// Type Guard Tests
// =============================================================================

describe('Type Guards', () => {
  describe('isMatchMode', () => {
    it('should return true for valid match modes', () => {
      expect(isMatchMode('exact')).toBe(true)
      expect(isMatchMode('fuzzy')).toBe(true)
    })

    it('should return false for invalid match modes', () => {
      expect(isMatchMode('unknown')).toBe(false)
      expect(isMatchMode('')).toBe(false)
      expect(isMatchMode(null)).toBe(false)
      expect(isMatchMode(undefined)).toBe(false)
      expect(isMatchMode(123)).toBe(false)
      expect(isMatchMode({ type: 'exact' })).toBe(false)
    })
  })

  describe('isValidSimilarity', () => {
    it('should return true for valid similarity scores', () => {
      expect(isValidSimilarity(0)).toBe(true)
      expect(isValidSimilarity(0.5)).toBe(true)
      expect(isValidSimilarity(1)).toBe(true)
      expect(isValidSimilarity(0.001)).toBe(true)
      expect(isValidSimilarity(0.999)).toBe(true)
    })

    it('should return false for invalid similarity scores', () => {
      expect(isValidSimilarity(-0.1)).toBe(false)
      expect(isValidSimilarity(1.1)).toBe(false)
      expect(isValidSimilarity(-1)).toBe(false)
      expect(isValidSimilarity(2)).toBe(false)
      expect(isValidSimilarity(null)).toBe(false)
      expect(isValidSimilarity(undefined)).toBe(false)
      expect(isValidSimilarity('0.5')).toBe(false)
      expect(isValidSimilarity(NaN)).toBe(false)
    })
  })
})

// =============================================================================
// Shredding Utility Tests
// =============================================================================

describe('Shredding Utilities', () => {
  describe('extractShreddedFields', () => {
    it('should extract matchMode and similarity as shredded fields', () => {
      const metadata: RelationshipMetadata = {
        matchMode: 'fuzzy',
        similarity: 0.85,
        matchedType: 'name-match',
        confidence: 'high',
        source: 'entity-resolution',
      }

      const { shredded, data } = extractShreddedFields(metadata)

      expect(shredded).toEqual({
        matchMode: 'fuzzy',
        similarity: 0.85,
      })
      expect(data).toEqual({
        matchedType: 'name-match',
        confidence: 'high',
        source: 'entity-resolution',
      })
    })

    it('should handle missing shredded fields', () => {
      const metadata: RelationshipMetadata = {
        matchedType: 'id-match',
        source: 'manual',
      }

      const { shredded, data } = extractShreddedFields(metadata)

      expect(shredded).toEqual({})
      expect(data).toEqual({
        matchedType: 'id-match',
        source: 'manual',
      })
    })

    it('should handle only matchMode present', () => {
      const metadata: RelationshipMetadata = {
        matchMode: 'exact',
        customField: 'value',
      }

      const { shredded, data } = extractShreddedFields(metadata)

      expect(shredded).toEqual({ matchMode: 'exact' })
      expect(data).toEqual({ customField: 'value' })
    })

    it('should handle only similarity present', () => {
      const metadata: RelationshipMetadata = {
        similarity: 0.95,
        customField: 'value',
      }

      const { shredded, data } = extractShreddedFields(metadata)

      expect(shredded).toEqual({ similarity: 0.95 })
      expect(data).toEqual({ customField: 'value' })
    })

    it('should handle empty metadata', () => {
      const metadata: RelationshipMetadata = {}

      const { shredded, data } = extractShreddedFields(metadata)

      expect(shredded).toEqual({})
      expect(data).toEqual({})
    })
  })

  describe('mergeShreddedFields', () => {
    it('should merge shredded fields with data', () => {
      const shredded: ShreddedRelationshipFields = {
        matchMode: 'fuzzy',
        similarity: 0.85,
      }
      const data = {
        matchedType: 'name-match',
        confidence: 'high',
      }

      const merged = mergeShreddedFields(shredded, data)

      expect(merged).toEqual({
        matchMode: 'fuzzy',
        similarity: 0.85,
        matchedType: 'name-match',
        confidence: 'high',
      })
    })

    it('should handle undefined data', () => {
      const shredded: ShreddedRelationshipFields = {
        matchMode: 'exact',
      }

      const merged = mergeShreddedFields(shredded, undefined)

      expect(merged).toEqual({
        matchMode: 'exact',
      })
    })

    it('should handle empty shredded fields', () => {
      const shredded: ShreddedRelationshipFields = {}
      const data = { source: 'manual' }

      const merged = mergeShreddedFields(shredded, data)

      expect(merged).toEqual({ source: 'manual' })
    })

    it('should roundtrip extract/merge', () => {
      const original: RelationshipMetadata = {
        matchMode: 'fuzzy',
        similarity: 0.75,
        matchedType: 'embedding',
        vector: [0.1, 0.2, 0.3],
      }

      const { shredded, data } = extractShreddedFields(original)
      const merged = mergeShreddedFields(shredded, data)

      expect(merged).toEqual(original)
    })
  })
})

// =============================================================================
// Validation Tests
// =============================================================================

describe('Validation', () => {
  describe('validateSimilarity', () => {
    it('should accept valid similarity scores', () => {
      expect(() => validateSimilarity(0)).not.toThrow()
      expect(() => validateSimilarity(0.5)).not.toThrow()
      expect(() => validateSimilarity(1)).not.toThrow()
    })

    it('should reject similarity below 0', () => {
      expect(() => validateSimilarity(-0.1)).toThrow(/between 0 and 1/)
      expect(() => validateSimilarity(-1)).toThrow(/between 0 and 1/)
    })

    it('should reject similarity above 1', () => {
      expect(() => validateSimilarity(1.1)).toThrow(/between 0 and 1/)
      expect(() => validateSimilarity(2)).toThrow(/between 0 and 1/)
    })
  })

  describe('validateMatchModeConsistency', () => {
    it('should accept exact mode without similarity', () => {
      expect(() => validateMatchModeConsistency('exact', undefined)).not.toThrow()
    })

    it('should accept exact mode with similarity of 1.0', () => {
      expect(() => validateMatchModeConsistency('exact', 1.0)).not.toThrow()
    })

    it('should reject exact mode with non-1.0 similarity', () => {
      expect(() => validateMatchModeConsistency('exact', 0.8)).toThrow(/Exact match mode/)
      expect(() => validateMatchModeConsistency('exact', 0)).toThrow(/Exact match mode/)
    })

    it('should reject fuzzy mode without similarity', () => {
      expect(() => validateMatchModeConsistency('fuzzy', undefined)).toThrow(/Fuzzy match mode requires/)
    })

    it('should accept fuzzy mode with valid similarity', () => {
      expect(() => validateMatchModeConsistency('fuzzy', 0.85)).not.toThrow()
      expect(() => validateMatchModeConsistency('fuzzy', 0.5)).not.toThrow()
      expect(() => validateMatchModeConsistency('fuzzy', 1.0)).not.toThrow()
    })

    it('should accept undefined matchMode with any similarity', () => {
      expect(() => validateMatchModeConsistency(undefined, 0.5)).not.toThrow()
      expect(() => validateMatchModeConsistency(undefined, undefined)).not.toThrow()
    })
  })
})

// =============================================================================
// Type Structure Tests
// =============================================================================

describe('Type Structures', () => {
  describe('RelationshipMetadata', () => {
    it('should allow all expected fields', () => {
      const metadata: RelationshipMetadata = {
        matchMode: 'fuzzy',
        similarity: 0.85,
        matchedType: 'name-match',
        confidence: 'high',
        source: 'entity-resolution',
        timestamp: Date.now(),
        nested: { value: 123 },
      }

      expect(metadata.matchMode).toBe('fuzzy')
      expect(metadata.similarity).toBe(0.85)
      expect(metadata.matchedType).toBe('name-match')
    })
  })

  describe('RelationshipFilter', () => {
    it('should support all filter fields', () => {
      const filter: RelationshipFilter = {
        fromNs: 'users',
        fromId: 'user-1',
        predicate: 'authored',
        toNs: 'posts',
        toId: 'post-1',
        matchMode: 'fuzzy',
        similarity: 0.8,
        minSimilarity: 0.7,
        maxSimilarity: 0.95,
      }

      expect(filter.matchMode).toBe('fuzzy')
      expect(filter.minSimilarity).toBe(0.7)
      expect(filter.maxSimilarity).toBe(0.95)
    })
  })

  describe('RelationshipWithMetadata', () => {
    it('should include shredded fields at top level', () => {
      const rel: RelationshipWithMetadata = {
        fromNs: 'users',
        fromId: 'user-1',
        predicate: 'knows',
        toNs: 'users',
        toId: 'user-2',
        matchMode: 'fuzzy',
        similarity: 0.92,
        createdAt: new Date(),
        version: 1,
        data: {
          matchedType: 'embedding',
          vector: [0.1, 0.2],
        },
      }

      // Shredded fields accessible at top level
      expect(rel.matchMode).toBe('fuzzy')
      expect(rel.similarity).toBe(0.92)

      // Remaining data in Variant column
      expect(rel.data?.matchedType).toBe('embedding')
    })
  })
})

// =============================================================================
// Edge Cases
// =============================================================================

describe('Edge Cases', () => {
  it('should handle similarity at boundary values', () => {
    const metadataZero: RelationshipMetadata = { similarity: 0 }
    const metadataOne: RelationshipMetadata = { similarity: 1 }

    const { shredded: s0 } = extractShreddedFields(metadataZero)
    const { shredded: s1 } = extractShreddedFields(metadataOne)

    expect(s0.similarity).toBe(0)
    expect(s1.similarity).toBe(1)
  })

  it('should handle undefined vs null correctly', () => {
    const metadata: RelationshipMetadata = {
      matchMode: undefined,
      similarity: undefined,
    }

    const { shredded } = extractShreddedFields(metadata)

    // undefined values should not be included in shredded
    expect('matchMode' in shredded).toBe(false)
    expect('similarity' in shredded).toBe(false)
  })

  it('should preserve data field order', () => {
    const metadata: RelationshipMetadata = {
      matchMode: 'fuzzy',
      aField: 1,
      bField: 2,
      similarity: 0.5,
      cField: 3,
    }

    const { data } = extractShreddedFields(metadata)
    const keys = Object.keys(data)

    expect(keys).toEqual(['aField', 'bField', 'cField'])
  })
})

// =============================================================================
// Integration Scenarios
// =============================================================================

describe('Integration Scenarios', () => {
  it('should support entity resolution use case', () => {
    // Simulating entity resolution creating fuzzy matches
    const fuzzyMatch: RelationshipMetadata = {
      matchMode: 'fuzzy',
      similarity: 0.87,
      matchedType: 'name-similarity',
      algorithms: ['levenshtein', 'jaro-winkler'],
      sourceRecord: 'external-db:record-123',
    }

    const { shredded, data } = extractShreddedFields(fuzzyMatch)

    // Query would filter by shredded fields first (uses column stats)
    expect(shredded.matchMode).toBe('fuzzy')
    expect(shredded.similarity).toBe(0.87)

    // Then load and parse remaining data from Variant
    expect(data.matchedType).toBe('name-similarity')
    expect(data.algorithms).toEqual(['levenshtein', 'jaro-winkler'])
  })

  it('should support user-created explicit links', () => {
    // User explicitly linking entities (no fuzzy matching)
    const explicitLink: RelationshipMetadata = {
      matchMode: 'exact',
      createdBy: 'users/admin',
      reason: 'Manually verified connection',
    }

    const { shredded, data } = extractShreddedFields(explicitLink)

    expect(shredded.matchMode).toBe('exact')
    expect(shredded.similarity).toBeUndefined()
    expect(data.createdBy).toBe('users/admin')
  })

  it('should support filtering high-confidence fuzzy matches', () => {
    // Simulating a query: "find all fuzzy matches with similarity > 0.9"
    const relationships: RelationshipWithMetadata[] = [
      {
        fromNs: 'entities', fromId: '1', predicate: 'sameAs',
        toNs: 'entities', toId: '2',
        matchMode: 'fuzzy', similarity: 0.95,
        createdAt: new Date(), version: 1,
      },
      {
        fromNs: 'entities', fromId: '1', predicate: 'sameAs',
        toNs: 'entities', toId: '3',
        matchMode: 'fuzzy', similarity: 0.75,
        createdAt: new Date(), version: 1,
      },
      {
        fromNs: 'entities', fromId: '1', predicate: 'sameAs',
        toNs: 'entities', toId: '4',
        matchMode: 'exact', similarity: undefined,
        createdAt: new Date(), version: 1,
      },
    ]

    // Filter using shredded fields (would use column stats in Parquet)
    const highConfidenceFuzzy = relationships.filter(r =>
      r.matchMode === 'fuzzy' && (r.similarity ?? 0) > 0.9
    )

    expect(highConfidenceFuzzy).toHaveLength(1)
    expect(highConfidenceFuzzy[0]?.toId).toBe('2')
    expect(highConfidenceFuzzy[0]?.similarity).toBe(0.95)
  })
})
