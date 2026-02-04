/**
 * Storage Mode Tests
 *
 * Tests for storage mode selection in worker endpoints.
 * Storage modes allow benchmarking different Parquet file layouts:
 * - columnar-only: Standard columnar layout for analytics
 * - row-index: With row group index for fast point lookups
 */

import { describe, it, expect } from 'vitest'
import {
  parseStorageMode,
  getCollectionPath,
  STORAGE_MODES,
  DEFAULT_STORAGE_MODE,
  QueryParamError,
  type StorageMode,
  type StorageModeConfig,
} from '@/worker/routing'

// =============================================================================
// Storage Mode Types and Constants Tests
// =============================================================================

describe('Storage Mode Types', () => {
  describe('STORAGE_MODES', () => {
    it('should have columnar-only mode', () => {
      expect(STORAGE_MODES['columnar-only']).toBeDefined()
      expect(STORAGE_MODES['columnar-only'].mode).toBe('columnar-only')
      expect(STORAGE_MODES['columnar-only'].pathTemplate).toContain('{dataset}')
    })

    it('should have row-index mode', () => {
      expect(STORAGE_MODES['row-index']).toBeDefined()
      expect(STORAGE_MODES['row-index'].mode).toBe('row-index')
      expect(STORAGE_MODES['row-index'].pathTemplate).toContain('{dataset}')
    })

    it('should have unique path templates for each mode', () => {
      const templates = Object.values(STORAGE_MODES).map(m => m.pathTemplate)
      const uniqueTemplates = new Set(templates)
      expect(uniqueTemplates.size).toBe(templates.length)
    })

    it('should have descriptions for all modes', () => {
      for (const mode of Object.values(STORAGE_MODES)) {
        expect(mode.description).toBeTruthy()
        expect(typeof mode.description).toBe('string')
      }
    })
  })

  describe('DEFAULT_STORAGE_MODE', () => {
    it('should be a valid storage mode', () => {
      expect(Object.keys(STORAGE_MODES)).toContain(DEFAULT_STORAGE_MODE)
    })

    it('should be columnar-only', () => {
      expect(DEFAULT_STORAGE_MODE).toBe('columnar-only')
    })
  })
})

// =============================================================================
// parseStorageMode Tests
// =============================================================================

describe('parseStorageMode', () => {
  describe('when mode is not specified', () => {
    it('should return null for empty params', () => {
      const params = new URLSearchParams()
      const result = parseStorageMode(params, 'imdb')
      expect(result).toBeNull()
    })

    it('should return null when mode param is missing', () => {
      const params = new URLSearchParams('limit=10&skip=5')
      const result = parseStorageMode(params, 'imdb')
      expect(result).toBeNull()
    })
  })

  describe('when mode is valid', () => {
    it('should parse columnar-only mode', () => {
      const params = new URLSearchParams('mode=columnar-only')
      const result = parseStorageMode(params, 'imdb')

      expect(result).not.toBeNull()
      expect(result!.mode).toBe('columnar-only')
      expect(result!.prefix).toBe('benchmarks/imdb-columnar-only')
      expect(result!.description).toBeTruthy()
    })

    it('should parse row-index mode', () => {
      const params = new URLSearchParams('mode=row-index')
      const result = parseStorageMode(params, 'imdb')

      expect(result).not.toBeNull()
      expect(result!.mode).toBe('row-index')
      expect(result!.prefix).toBe('benchmarks/imdb-row-index')
      expect(result!.description).toBeTruthy()
    })

    it('should replace {dataset} placeholder with actual dataset ID', () => {
      const params = new URLSearchParams('mode=columnar-only')

      const imdbResult = parseStorageMode(params, 'imdb')
      expect(imdbResult!.prefix).toBe('benchmarks/imdb-columnar-only')

      const onetResult = parseStorageMode(params, 'onet')
      expect(onetResult!.prefix).toBe('benchmarks/onet-columnar-only')

      const imdb1mResult = parseStorageMode(params, 'imdb-1m')
      expect(imdb1mResult!.prefix).toBe('benchmarks/imdb-1m-columnar-only')
    })

    it('should work with other query params', () => {
      const params = new URLSearchParams('limit=100&mode=row-index&skip=50')
      const result = parseStorageMode(params, 'imdb')

      expect(result).not.toBeNull()
      expect(result!.mode).toBe('row-index')
    })
  })

  describe('when mode is invalid', () => {
    it('should throw QueryParamError for unknown mode', () => {
      const params = new URLSearchParams('mode=unknown')

      expect(() => parseStorageMode(params, 'imdb')).toThrow(QueryParamError)
      expect(() => parseStorageMode(params, 'imdb')).toThrow(/Invalid mode/)
    })

    it('should include valid modes in error message', () => {
      const params = new URLSearchParams('mode=invalid')

      try {
        parseStorageMode(params, 'imdb')
        expect.fail('Should have thrown')
      } catch (error) {
        expect(error).toBeInstanceOf(QueryParamError)
        expect((error as QueryParamError).message).toContain('columnar-only')
        expect((error as QueryParamError).message).toContain('row-index')
      }
    })

    it('should throw for empty mode value', () => {
      // Note: URLSearchParams with ?mode= (empty value) will have mode=''
      // This is different from ?other=value (mode missing)
      const params = new URLSearchParams('mode=')

      // Empty string is not a valid mode, should throw
      expect(() => parseStorageMode(params, 'imdb')).toThrow(QueryParamError)
    })

    it('should throw for case-sensitive mismatch', () => {
      const params = new URLSearchParams('mode=Columnar-Only')

      expect(() => parseStorageMode(params, 'imdb')).toThrow(QueryParamError)
    })

    it('should throw for partial mode names', () => {
      const params = new URLSearchParams('mode=columnar')

      expect(() => parseStorageMode(params, 'imdb')).toThrow(QueryParamError)
    })
  })
})

// =============================================================================
// getCollectionPath Tests
// =============================================================================

describe('getCollectionPath', () => {
  describe('without storage mode', () => {
    it('should use default dataset prefix', () => {
      const path = getCollectionPath('imdb', 'titles', null)
      expect(path).toBe('imdb/titles.parquet')
    })

    it('should work with different collections', () => {
      expect(getCollectionPath('imdb', 'titles', null)).toBe('imdb/titles.parquet')
      expect(getCollectionPath('imdb', 'people', null)).toBe('imdb/people.parquet')
      expect(getCollectionPath('onet', 'occupations', null)).toBe('onet/occupations.parquet')
    })

    it('should handle complex prefix paths', () => {
      const path = getCollectionPath('onet-graph', 'skills', null)
      expect(path).toBe('onet-graph/skills.parquet')
    })
  })

  describe('with storage mode', () => {
    it('should use storage mode prefix for columnar-only', () => {
      const storageMode: StorageModeConfig = {
        mode: 'columnar-only',
        prefix: 'benchmarks/imdb-columnar-only',
        description: 'Test mode',
      }

      const path = getCollectionPath('imdb', 'titles', storageMode)
      expect(path).toBe('benchmarks/imdb-columnar-only/titles.parquet')
    })

    it('should use storage mode prefix for row-index', () => {
      const storageMode: StorageModeConfig = {
        mode: 'row-index',
        prefix: 'benchmarks/imdb-row-index',
        description: 'Test mode',
      }

      const path = getCollectionPath('imdb', 'titles', storageMode)
      expect(path).toBe('benchmarks/imdb-row-index/titles.parquet')
    })

    it('should ignore default prefix when storage mode is specified', () => {
      const storageMode: StorageModeConfig = {
        mode: 'columnar-only',
        prefix: 'benchmarks/custom-path',
        description: 'Custom prefix',
      }

      // Even though datasetPrefix is 'imdb', the storage mode prefix should be used
      const path = getCollectionPath('imdb', 'titles', storageMode)
      expect(path).toBe('benchmarks/custom-path/titles.parquet')
    })
  })
})

// =============================================================================
// Integration Tests
// =============================================================================

describe('Storage Mode Integration', () => {
  it('should produce correct paths for all modes and datasets', () => {
    const datasets = ['imdb', 'imdb-1m', 'onet', 'onet-full', 'unspsc']
    const collections = ['titles', 'occupations', 'commodities']

    for (const dataset of datasets) {
      for (const mode of Object.keys(STORAGE_MODES) as StorageMode[]) {
        const params = new URLSearchParams(`mode=${mode}`)
        const storageMode = parseStorageMode(params, dataset)

        expect(storageMode).not.toBeNull()
        expect(storageMode!.mode).toBe(mode)

        // Verify prefix format
        expect(storageMode!.prefix).toMatch(new RegExp(`^benchmarks/${dataset}-${mode}$`))

        // Verify collection paths
        for (const collection of collections) {
          const path = getCollectionPath(dataset, collection, storageMode)
          expect(path).toBe(`benchmarks/${dataset}-${mode}/${collection}.parquet`)
        }
      }
    }
  })

  it('should handle full URL parsing flow', () => {
    // Simulate parsing a full URL
    const url = new URL('https://api.parquedb.com/datasets/imdb/titles?mode=row-index&limit=100')
    const params = url.searchParams

    const storageMode = parseStorageMode(params, 'imdb')
    expect(storageMode).not.toBeNull()

    const path = getCollectionPath('imdb', 'titles', storageMode)
    expect(path).toBe('benchmarks/imdb-row-index/titles.parquet')
  })

  it('should fall back to default prefix when no mode specified', () => {
    const url = new URL('https://api.parquedb.com/datasets/imdb/titles?limit=100')
    const params = url.searchParams

    const storageMode = parseStorageMode(params, 'imdb')
    expect(storageMode).toBeNull()

    // When storageMode is null, getCollectionPath uses the default prefix
    const path = getCollectionPath('imdb', 'titles', storageMode)
    expect(path).toBe('imdb/titles.parquet')
  })
})

// =============================================================================
// Error Handling Tests
// =============================================================================

describe('Storage Mode Error Handling', () => {
  it('QueryParamError should have correct structure', () => {
    const params = new URLSearchParams('mode=invalid')

    try {
      parseStorageMode(params, 'imdb')
      expect.fail('Should have thrown')
    } catch (error) {
      expect(error).toBeInstanceOf(QueryParamError)
      expect((error as QueryParamError).status).toBe(400)
      expect((error as QueryParamError).name).toBe('QueryParamError')
    }
  })

  it('should provide helpful error message with valid options', () => {
    const params = new URLSearchParams('mode=typo-mode')

    try {
      parseStorageMode(params, 'imdb')
      expect.fail('Should have thrown')
    } catch (error) {
      const message = (error as QueryParamError).message
      expect(message).toContain("Invalid mode 'typo-mode'")
      expect(message).toContain('Valid modes:')
      // Should list all valid modes
      for (const mode of Object.keys(STORAGE_MODES)) {
        expect(message).toContain(mode)
      }
    }
  })
})
