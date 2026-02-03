/**
 * Vector Search Benchmarks for ParqueDB
 *
 * Comprehensive performance benchmarks for HNSW-based vector similarity search:
 * - Index build time at various scales (1K, 10K, 100K vectors)
 * - Search performance across different dimensions (128, 768, 1024, 1536)
 * - Distance metrics comparison (cosine, euclidean, dot)
 * - Hybrid search (pre-filter vs post-filter strategies)
 * - Memory-bounded operation performance
 *
 * Performance Targets:
 * - Vector search (10K): 5ms p50, 20ms p99
 * - Vector search (100K): 15ms p50, 50ms p99
 */

import { describe, bench, beforeAll, beforeEach, afterAll } from 'vitest'
import { VectorIndex } from '../../src/indexes/vector/hnsw'
import { MemoryBackend } from '../../src/storage/MemoryBackend'
import type { IndexDefinition, VectorMetric } from '../../src/indexes/types'
import {
  generateId,
  randomInt,
  startTimer,
  calculateStats,
  formatStats,
} from './setup'

// =============================================================================
// Test Utilities
// =============================================================================

/**
 * Generate a random normalized vector of given dimensions
 */
function generateVector(dimensions: number): number[] {
  const vector: number[] = []
  let magnitude = 0

  for (let i = 0; i < dimensions; i++) {
    const v = Math.random() * 2 - 1 // Random value between -1 and 1
    vector.push(v)
    magnitude += v * v
  }

  // Normalize the vector
  magnitude = Math.sqrt(magnitude)
  if (magnitude > 0) {
    for (let i = 0; i < dimensions; i++) {
      vector[i] = vector[i]! / magnitude
    }
  }

  return vector
}

/**
 * Generate test data with vectors
 */
function generateVectorData(
  count: number,
  dimensions: number
): Array<{
  doc: Record<string, unknown>
  docId: string
  rowGroup: number
  rowOffset: number
}> {
  const data: Array<{
    doc: Record<string, unknown>
    docId: string
    rowGroup: number
    rowOffset: number
  }> = []

  const rowGroupSize = 1000
  for (let i = 0; i < count; i++) {
    const rowGroup = Math.floor(i / rowGroupSize)
    const rowOffset = i % rowGroupSize

    data.push({
      doc: {
        $id: `vectors/vec-${i}`,
        $type: 'Vector',
        name: `Vector ${i}`,
        embedding: generateVector(dimensions),
        category: ['A', 'B', 'C', 'D', 'E'][randomInt(0, 4)],
        score: Math.random(),
      },
      docId: `vectors/vec-${i}`,
      rowGroup,
      rowOffset,
    })
  }

  return data
}

/**
 * Create a VectorIndex with given parameters
 */
function createVectorIndex(
  storage: MemoryBackend,
  dimensions: number,
  metric: VectorMetric = 'cosine',
  m: number = 16,
  efConstruction: number = 200
): VectorIndex {
  const definition: IndexDefinition = {
    name: 'embedding',
    type: 'vector',
    fields: [{ path: 'embedding' }],
    vectorOptions: {
      dimensions,
      metric,
      m,
      efConstruction,
    },
  }

  return new VectorIndex(storage, 'vectors', definition, '', {
    maxNodes: 200000,
    maxBytes: 512 * 1024 * 1024, // 512MB
  })
}

// =============================================================================
// Vector Search Benchmarks
// =============================================================================

describe('Vector Search Benchmarks', () => {
  // ===========================================================================
  // Scale Tests: 1K, 10K, 100K vectors
  // ===========================================================================

  describe('Scale Tests (128 dimensions, cosine)', () => {
    const DIMENSIONS = 128

    describe('1K Vectors', () => {
      let index: VectorIndex
      let storage: MemoryBackend
      let queryVectors: number[][] = []
      const VECTOR_COUNT = 1000

      beforeAll(async () => {
        storage = new MemoryBackend()
        index = createVectorIndex(storage, DIMENSIONS)

        // Build index
        const data = generateVectorData(VECTOR_COUNT, DIMENSIONS)
        index.buildFromArray(data)

        // Pre-generate query vectors
        for (let i = 0; i < 100; i++) {
          queryVectors.push(generateVector(DIMENSIONS))
        }
      })

      bench('build index (1K vectors)', async () => {
        const newStorage = new MemoryBackend()
        const newIndex = createVectorIndex(newStorage, DIMENSIONS)
        const data = generateVectorData(VECTOR_COUNT, DIMENSIONS)
        newIndex.buildFromArray(data)
      }, { iterations: 10 })

      bench('search k=10 (1K vectors)', async () => {
        const query = queryVectors[randomInt(0, queryVectors.length - 1)]!
        index.search(query, 10)
      })

      bench('search k=50 (1K vectors)', async () => {
        const query = queryVectors[randomInt(0, queryVectors.length - 1)]!
        index.search(query, 50)
      })

      bench('search k=100 (1K vectors)', async () => {
        const query = queryVectors[randomInt(0, queryVectors.length - 1)]!
        index.search(query, 100)
      })

      bench('search with minScore filter (1K vectors)', async () => {
        const query = queryVectors[randomInt(0, queryVectors.length - 1)]!
        index.search(query, 10, { minScore: 0.7 })
      })
    })

    describe('10K Vectors', () => {
      let index: VectorIndex
      let storage: MemoryBackend
      let queryVectors: number[][] = []
      const VECTOR_COUNT = 10000

      beforeAll(async () => {
        storage = new MemoryBackend()
        index = createVectorIndex(storage, DIMENSIONS)

        // Build index
        const data = generateVectorData(VECTOR_COUNT, DIMENSIONS)
        index.buildFromArray(data)

        // Pre-generate query vectors
        for (let i = 0; i < 100; i++) {
          queryVectors.push(generateVector(DIMENSIONS))
        }
      })

      bench('build index (10K vectors)', async () => {
        const newStorage = new MemoryBackend()
        const newIndex = createVectorIndex(newStorage, DIMENSIONS)
        const data = generateVectorData(VECTOR_COUNT, DIMENSIONS)
        newIndex.buildFromArray(data)
      }, { iterations: 5 })

      bench('search k=10 (10K vectors)', async () => {
        const query = queryVectors[randomInt(0, queryVectors.length - 1)]!
        index.search(query, 10)
      })

      bench('search k=50 (10K vectors)', async () => {
        const query = queryVectors[randomInt(0, queryVectors.length - 1)]!
        index.search(query, 50)
      })

      bench('search k=100 (10K vectors)', async () => {
        const query = queryVectors[randomInt(0, queryVectors.length - 1)]!
        index.search(query, 100)
      })

      bench('search with minScore filter (10K vectors)', async () => {
        const query = queryVectors[randomInt(0, queryVectors.length - 1)]!
        index.search(query, 10, { minScore: 0.7 })
      })

      bench('search with high efSearch (10K vectors)', async () => {
        const query = queryVectors[randomInt(0, queryVectors.length - 1)]!
        index.search(query, 10, { efSearch: 200 })
      })
    })

    describe('100K Vectors', () => {
      let index: VectorIndex
      let storage: MemoryBackend
      let queryVectors: number[][] = []
      const VECTOR_COUNT = 100000

      beforeAll(async () => {
        storage = new MemoryBackend()
        index = createVectorIndex(storage, DIMENSIONS)

        // Build index
        const data = generateVectorData(VECTOR_COUNT, DIMENSIONS)
        index.buildFromArray(data)

        // Pre-generate query vectors
        for (let i = 0; i < 100; i++) {
          queryVectors.push(generateVector(DIMENSIONS))
        }
      }, { timeout: 120000 }) // 2 minute timeout for setup

      bench('build index (100K vectors)', async () => {
        const newStorage = new MemoryBackend()
        const newIndex = createVectorIndex(newStorage, DIMENSIONS)
        const data = generateVectorData(VECTOR_COUNT, DIMENSIONS)
        newIndex.buildFromArray(data)
      }, { iterations: 3, timeout: 60000 })

      bench('search k=10 (100K vectors)', async () => {
        const query = queryVectors[randomInt(0, queryVectors.length - 1)]!
        index.search(query, 10)
      })

      bench('search k=50 (100K vectors)', async () => {
        const query = queryVectors[randomInt(0, queryVectors.length - 1)]!
        index.search(query, 50)
      })

      bench('search k=100 (100K vectors)', async () => {
        const query = queryVectors[randomInt(0, queryVectors.length - 1)]!
        index.search(query, 100)
      })

      bench('search with minScore filter (100K vectors)', async () => {
        const query = queryVectors[randomInt(0, queryVectors.length - 1)]!
        index.search(query, 10, { minScore: 0.7 })
      })

      bench('search with high efSearch (100K vectors)', async () => {
        const query = queryVectors[randomInt(0, queryVectors.length - 1)]!
        index.search(query, 10, { efSearch: 200 })
      })
    })
  })

  // ===========================================================================
  // Dimension Tests: 128, 768, 1024, 1536
  // ===========================================================================

  describe('Dimension Tests (10K vectors, cosine)', () => {
    const VECTOR_COUNT = 10000

    describe('128 Dimensions', () => {
      const DIMENSIONS = 128
      let index: VectorIndex
      let storage: MemoryBackend
      let queryVectors: number[][] = []

      beforeAll(async () => {
        storage = new MemoryBackend()
        index = createVectorIndex(storage, DIMENSIONS)
        const data = generateVectorData(VECTOR_COUNT, DIMENSIONS)
        index.buildFromArray(data)

        for (let i = 0; i < 50; i++) {
          queryVectors.push(generateVector(DIMENSIONS))
        }
      })

      bench('search k=10 (128-dim)', async () => {
        const query = queryVectors[randomInt(0, queryVectors.length - 1)]!
        index.search(query, 10)
      })

      bench('insert single vector (128-dim)', async () => {
        const vector = generateVector(DIMENSIONS)
        const docId = generateId('vec')
        index.insert(vector, docId, 999, 0)
      })
    })

    describe('768 Dimensions (BERT-sized)', () => {
      const DIMENSIONS = 768
      let index: VectorIndex
      let storage: MemoryBackend
      let queryVectors: number[][] = []

      beforeAll(async () => {
        storage = new MemoryBackend()
        index = createVectorIndex(storage, DIMENSIONS)
        const data = generateVectorData(VECTOR_COUNT, DIMENSIONS)
        index.buildFromArray(data)

        for (let i = 0; i < 50; i++) {
          queryVectors.push(generateVector(DIMENSIONS))
        }
      }, { timeout: 60000 })

      bench('search k=10 (768-dim)', async () => {
        const query = queryVectors[randomInt(0, queryVectors.length - 1)]!
        index.search(query, 10)
      })

      bench('insert single vector (768-dim)', async () => {
        const vector = generateVector(DIMENSIONS)
        const docId = generateId('vec')
        index.insert(vector, docId, 999, 0)
      })
    })

    describe('1024 Dimensions', () => {
      const DIMENSIONS = 1024
      let index: VectorIndex
      let storage: MemoryBackend
      let queryVectors: number[][] = []

      beforeAll(async () => {
        storage = new MemoryBackend()
        index = createVectorIndex(storage, DIMENSIONS)
        const data = generateVectorData(VECTOR_COUNT, DIMENSIONS)
        index.buildFromArray(data)

        for (let i = 0; i < 50; i++) {
          queryVectors.push(generateVector(DIMENSIONS))
        }
      }, { timeout: 60000 })

      bench('search k=10 (1024-dim)', async () => {
        const query = queryVectors[randomInt(0, queryVectors.length - 1)]!
        index.search(query, 10)
      })

      bench('insert single vector (1024-dim)', async () => {
        const vector = generateVector(DIMENSIONS)
        const docId = generateId('vec')
        index.insert(vector, docId, 999, 0)
      })
    })

    describe('1536 Dimensions (OpenAI text-embedding-3-small)', () => {
      const DIMENSIONS = 1536
      let index: VectorIndex
      let storage: MemoryBackend
      let queryVectors: number[][] = []

      beforeAll(async () => {
        storage = new MemoryBackend()
        index = createVectorIndex(storage, DIMENSIONS)
        const data = generateVectorData(VECTOR_COUNT, DIMENSIONS)
        index.buildFromArray(data)

        for (let i = 0; i < 50; i++) {
          queryVectors.push(generateVector(DIMENSIONS))
        }
      }, { timeout: 90000 })

      bench('search k=10 (1536-dim)', async () => {
        const query = queryVectors[randomInt(0, queryVectors.length - 1)]!
        index.search(query, 10)
      })

      bench('insert single vector (1536-dim)', async () => {
        const vector = generateVector(DIMENSIONS)
        const docId = generateId('vec')
        index.insert(vector, docId, 999, 0)
      })
    })
  })

  // ===========================================================================
  // Distance Metric Tests: cosine, euclidean, dot
  // ===========================================================================

  describe('Distance Metric Comparison (10K vectors, 128-dim)', () => {
    const DIMENSIONS = 128
    const VECTOR_COUNT = 10000

    describe('Cosine Distance', () => {
      let index: VectorIndex
      let storage: MemoryBackend
      let queryVectors: number[][] = []

      beforeAll(async () => {
        storage = new MemoryBackend()
        index = createVectorIndex(storage, DIMENSIONS, 'cosine')
        const data = generateVectorData(VECTOR_COUNT, DIMENSIONS)
        index.buildFromArray(data)

        for (let i = 0; i < 50; i++) {
          queryVectors.push(generateVector(DIMENSIONS))
        }
      })

      bench('search k=10 (cosine)', async () => {
        const query = queryVectors[randomInt(0, queryVectors.length - 1)]!
        index.search(query, 10)
      })

      bench('search k=50 (cosine)', async () => {
        const query = queryVectors[randomInt(0, queryVectors.length - 1)]!
        index.search(query, 50)
      })
    })

    describe('Euclidean Distance', () => {
      let index: VectorIndex
      let storage: MemoryBackend
      let queryVectors: number[][] = []

      beforeAll(async () => {
        storage = new MemoryBackend()
        index = createVectorIndex(storage, DIMENSIONS, 'euclidean')
        const data = generateVectorData(VECTOR_COUNT, DIMENSIONS)
        index.buildFromArray(data)

        for (let i = 0; i < 50; i++) {
          queryVectors.push(generateVector(DIMENSIONS))
        }
      })

      bench('search k=10 (euclidean)', async () => {
        const query = queryVectors[randomInt(0, queryVectors.length - 1)]!
        index.search(query, 10)
      })

      bench('search k=50 (euclidean)', async () => {
        const query = queryVectors[randomInt(0, queryVectors.length - 1)]!
        index.search(query, 50)
      })
    })

    describe('Dot Product Distance', () => {
      let index: VectorIndex
      let storage: MemoryBackend
      let queryVectors: number[][] = []

      beforeAll(async () => {
        storage = new MemoryBackend()
        index = createVectorIndex(storage, DIMENSIONS, 'dot')
        const data = generateVectorData(VECTOR_COUNT, DIMENSIONS)
        index.buildFromArray(data)

        for (let i = 0; i < 50; i++) {
          queryVectors.push(generateVector(DIMENSIONS))
        }
      })

      bench('search k=10 (dot product)', async () => {
        const query = queryVectors[randomInt(0, queryVectors.length - 1)]!
        index.search(query, 10)
      })

      bench('search k=50 (dot product)', async () => {
        const query = queryVectors[randomInt(0, queryVectors.length - 1)]!
        index.search(query, 50)
      })
    })
  })

  // ===========================================================================
  // Hybrid Search Tests
  // ===========================================================================

  describe('Hybrid Search (10K vectors, 128-dim)', () => {
    const DIMENSIONS = 128
    const VECTOR_COUNT = 10000
    let index: VectorIndex
    let storage: MemoryBackend
    let queryVectors: number[][] = []
    let allDocIds: Set<string>
    let smallCandidateSet: Set<string>
    let mediumCandidateSet: Set<string>
    let largeCandidateSet: Set<string>

    beforeAll(async () => {
      storage = new MemoryBackend()
      index = createVectorIndex(storage, DIMENSIONS)
      const data = generateVectorData(VECTOR_COUNT, DIMENSIONS)
      index.buildFromArray(data)

      // Pre-generate query vectors
      for (let i = 0; i < 50; i++) {
        queryVectors.push(generateVector(DIMENSIONS))
      }

      // Create candidate sets of different sizes
      allDocIds = index.getAllDocIds()
      const docIdArray = Array.from(allDocIds)

      // Small: 100 docs (1%)
      smallCandidateSet = new Set(docIdArray.slice(0, 100))

      // Medium: 1000 docs (10%)
      mediumCandidateSet = new Set(docIdArray.slice(0, 1000))

      // Large: 5000 docs (50%)
      largeCandidateSet = new Set(docIdArray.slice(0, 5000))
    })

    bench('hybrid search - pre-filter (small candidate set, 1%)', async () => {
      const query = queryVectors[randomInt(0, queryVectors.length - 1)]!
      index.hybridSearch(query, 10, {
        strategy: 'pre-filter',
        candidateIds: smallCandidateSet,
      })
    })

    bench('hybrid search - pre-filter (medium candidate set, 10%)', async () => {
      const query = queryVectors[randomInt(0, queryVectors.length - 1)]!
      index.hybridSearch(query, 10, {
        strategy: 'pre-filter',
        candidateIds: mediumCandidateSet,
      })
    })

    bench('hybrid search - pre-filter (large candidate set, 50%)', async () => {
      const query = queryVectors[randomInt(0, queryVectors.length - 1)]!
      index.hybridSearch(query, 10, {
        strategy: 'pre-filter',
        candidateIds: largeCandidateSet,
      })
    })

    bench('hybrid search - post-filter (over-fetch 3x)', async () => {
      const query = queryVectors[randomInt(0, queryVectors.length - 1)]!
      index.hybridSearch(query, 10, {
        strategy: 'post-filter',
        overFetchMultiplier: 3,
      })
    })

    bench('hybrid search - post-filter (over-fetch 5x)', async () => {
      const query = queryVectors[randomInt(0, queryVectors.length - 1)]!
      index.hybridSearch(query, 10, {
        strategy: 'post-filter',
        overFetchMultiplier: 5,
      })
    })

    bench('hybrid search - auto strategy (small candidate set)', async () => {
      const query = queryVectors[randomInt(0, queryVectors.length - 1)]!
      index.hybridSearch(query, 10, {
        strategy: 'auto',
        candidateIds: smallCandidateSet,
      })
    })

    bench('hybrid search - auto strategy (medium candidate set)', async () => {
      const query = queryVectors[randomInt(0, queryVectors.length - 1)]!
      index.hybridSearch(query, 10, {
        strategy: 'auto',
        candidateIds: mediumCandidateSet,
      })
    })

    bench('hybrid search - auto strategy (large candidate set)', async () => {
      const query = queryVectors[randomInt(0, queryVectors.length - 1)]!
      index.hybridSearch(query, 10, {
        strategy: 'auto',
        candidateIds: largeCandidateSet,
      })
    })
  })

  // ===========================================================================
  // HNSW Parameter Tuning Tests
  // ===========================================================================

  describe('HNSW Parameter Tuning (10K vectors, 128-dim)', () => {
    const DIMENSIONS = 128
    const VECTOR_COUNT = 10000

    describe('M parameter (connections per node)', () => {
      let queryVectors: number[][] = []

      beforeAll(() => {
        for (let i = 0; i < 50; i++) {
          queryVectors.push(generateVector(DIMENSIONS))
        }
      })

      bench('M=8 (lower connectivity)', async () => {
        const storage = new MemoryBackend()
        const index = createVectorIndex(storage, DIMENSIONS, 'cosine', 8, 100)
        const data = generateVectorData(VECTOR_COUNT, DIMENSIONS)
        index.buildFromArray(data)

        // Perform searches
        for (let i = 0; i < 10; i++) {
          const query = queryVectors[randomInt(0, queryVectors.length - 1)]!
          index.search(query, 10)
        }
      }, { iterations: 3 })

      bench('M=16 (default)', async () => {
        const storage = new MemoryBackend()
        const index = createVectorIndex(storage, DIMENSIONS, 'cosine', 16, 100)
        const data = generateVectorData(VECTOR_COUNT, DIMENSIONS)
        index.buildFromArray(data)

        // Perform searches
        for (let i = 0; i < 10; i++) {
          const query = queryVectors[randomInt(0, queryVectors.length - 1)]!
          index.search(query, 10)
        }
      }, { iterations: 3 })

      bench('M=32 (higher connectivity)', async () => {
        const storage = new MemoryBackend()
        const index = createVectorIndex(storage, DIMENSIONS, 'cosine', 32, 100)
        const data = generateVectorData(VECTOR_COUNT, DIMENSIONS)
        index.buildFromArray(data)

        // Perform searches
        for (let i = 0; i < 10; i++) {
          const query = queryVectors[randomInt(0, queryVectors.length - 1)]!
          index.search(query, 10)
        }
      }, { iterations: 3 })
    })

    describe('efSearch parameter (search beam width)', () => {
      let index: VectorIndex
      let storage: MemoryBackend
      let queryVectors: number[][] = []

      beforeAll(async () => {
        storage = new MemoryBackend()
        index = createVectorIndex(storage, DIMENSIONS)
        const data = generateVectorData(VECTOR_COUNT, DIMENSIONS)
        index.buildFromArray(data)

        for (let i = 0; i < 50; i++) {
          queryVectors.push(generateVector(DIMENSIONS))
        }
      })

      bench('efSearch=20 (fast, lower recall)', async () => {
        const query = queryVectors[randomInt(0, queryVectors.length - 1)]!
        index.search(query, 10, { efSearch: 20 })
      })

      bench('efSearch=50 (balanced)', async () => {
        const query = queryVectors[randomInt(0, queryVectors.length - 1)]!
        index.search(query, 10, { efSearch: 50 })
      })

      bench('efSearch=100 (default)', async () => {
        const query = queryVectors[randomInt(0, queryVectors.length - 1)]!
        index.search(query, 10, { efSearch: 100 })
      })

      bench('efSearch=200 (high recall)', async () => {
        const query = queryVectors[randomInt(0, queryVectors.length - 1)]!
        index.search(query, 10, { efSearch: 200 })
      })

      bench('efSearch=500 (maximum recall)', async () => {
        const query = queryVectors[randomInt(0, queryVectors.length - 1)]!
        index.search(query, 10, { efSearch: 500 })
      })
    })
  })

  // ===========================================================================
  // Index Modification Operations
  // ===========================================================================

  describe('Index Modification (10K vectors, 128-dim)', () => {
    const DIMENSIONS = 128
    const VECTOR_COUNT = 10000

    describe('Insert Operations', () => {
      let index: VectorIndex
      let storage: MemoryBackend

      beforeEach(async () => {
        storage = new MemoryBackend()
        index = createVectorIndex(storage, DIMENSIONS)
        const data = generateVectorData(VECTOR_COUNT, DIMENSIONS)
        index.buildFromArray(data)
      })

      bench('insert single vector', async () => {
        const vector = generateVector(DIMENSIONS)
        const docId = generateId('new-vec')
        index.insert(vector, docId, 999, randomInt(0, 999))
      })

      bench('insert 10 vectors sequentially', async () => {
        for (let i = 0; i < 10; i++) {
          const vector = generateVector(DIMENSIONS)
          const docId = generateId('batch-vec')
          index.insert(vector, docId, 999, i)
        }
      })

      bench('insert 100 vectors sequentially', async () => {
        for (let i = 0; i < 100; i++) {
          const vector = generateVector(DIMENSIONS)
          const docId = generateId('batch-vec')
          index.insert(vector, docId, 999, i)
        }
      })
    })

    describe('Remove Operations', () => {
      let index: VectorIndex
      let storage: MemoryBackend
      let docIds: string[] = []

      beforeEach(async () => {
        storage = new MemoryBackend()
        index = createVectorIndex(storage, DIMENSIONS)
        const data = generateVectorData(VECTOR_COUNT, DIMENSIONS)
        index.buildFromArray(data)
        docIds = data.map(d => d.docId)
      })

      bench('remove single vector', async () => {
        const docId = docIds[randomInt(0, docIds.length - 1)]!
        index.remove(docId)
      })

      bench('remove 10 vectors sequentially', async () => {
        for (let i = 0; i < 10; i++) {
          const docId = docIds[randomInt(0, docIds.length - 1)]!
          index.remove(docId)
        }
      })
    })

    describe('Update Operations', () => {
      let index: VectorIndex
      let storage: MemoryBackend
      let docIds: string[] = []

      beforeEach(async () => {
        storage = new MemoryBackend()
        index = createVectorIndex(storage, DIMENSIONS)
        const data = generateVectorData(VECTOR_COUNT, DIMENSIONS)
        index.buildFromArray(data)
        docIds = data.map(d => d.docId)
      })

      bench('update single vector', async () => {
        const docId = docIds[randomInt(0, docIds.length - 1)]!
        const newVector = generateVector(DIMENSIONS)
        index.update(newVector, docId, 0, 0)
      })

      bench('update 10 vectors sequentially', async () => {
        for (let i = 0; i < 10; i++) {
          const docId = docIds[randomInt(0, docIds.length - 1)]!
          const newVector = generateVector(DIMENSIONS)
          index.update(newVector, docId, 0, i)
        }
      })
    })
  })

  // ===========================================================================
  // Persistence Operations
  // ===========================================================================

  describe('Persistence Operations', () => {
    const DIMENSIONS = 128

    describe('Save/Load (1K vectors)', () => {
      const VECTOR_COUNT = 1000
      let storage: MemoryBackend
      let index: VectorIndex

      beforeAll(async () => {
        storage = new MemoryBackend()
        index = createVectorIndex(storage, DIMENSIONS)
        const data = generateVectorData(VECTOR_COUNT, DIMENSIONS)
        index.buildFromArray(data)
      })

      bench('save index (1K vectors)', async () => {
        await index.save()
      })

      bench('load index (1K vectors)', async () => {
        const newIndex = createVectorIndex(storage, DIMENSIONS)
        await newIndex.load()
      })
    })

    describe('Save/Load (10K vectors)', () => {
      const VECTOR_COUNT = 10000
      let storage: MemoryBackend
      let index: VectorIndex

      beforeAll(async () => {
        storage = new MemoryBackend()
        index = createVectorIndex(storage, DIMENSIONS)
        const data = generateVectorData(VECTOR_COUNT, DIMENSIONS)
        index.buildFromArray(data)
      })

      bench('save index (10K vectors)', async () => {
        await index.save()
      }, { iterations: 5 })

      bench('load index (10K vectors)', async () => {
        // First save to ensure data exists
        await index.save()
        const newIndex = createVectorIndex(storage, DIMENSIONS)
        await newIndex.load()
      }, { iterations: 5 })
    })
  })

  // ===========================================================================
  // Incremental Update Operations
  // ===========================================================================

  describe('Incremental Update Operations', () => {
    const DIMENSIONS = 128
    const VECTOR_COUNT = 10000

    describe('Row Group Updates', () => {
      let index: VectorIndex
      let storage: MemoryBackend

      beforeEach(async () => {
        storage = new MemoryBackend()
        index = createVectorIndex(storage, DIMENSIONS)
        const data = generateVectorData(VECTOR_COUNT, DIMENSIONS)
        index.buildFromArray(data)
      })

      bench('remove single row group (1000 vectors)', async () => {
        index.removeRowGroup(0)
      })

      bench('detect changed row groups', async () => {
        const checksums = new Map<number, string>()
        for (let i = 0; i < 10; i++) {
          checksums.set(i, `checksum-${i}`)
        }
        // Add a new row group and modify one
        checksums.set(10, 'new-checksum')
        checksums.set(0, 'modified-checksum')
        index.detectChangedRowGroups(checksums)
      })

      bench('remap row groups (after compaction)', async () => {
        const remapping = new Map<number, number>()
        for (let i = 0; i < 10; i++) {
          remapping.set(i, i) // Keep same mapping
        }
        index.remapRowGroups(remapping)
      })
    })

    describe('Incremental Update from Array', () => {
      let index: VectorIndex
      let storage: MemoryBackend

      beforeEach(async () => {
        storage = new MemoryBackend()
        index = createVectorIndex(storage, DIMENSIONS)
        const data = generateVectorData(VECTOR_COUNT, DIMENSIONS)
        index.buildFromArray(data)
      })

      bench('incremental update - add 100 vectors', async () => {
        const newData = generateVectorData(100, DIMENSIONS).map(d => ({
          ...d,
          rowGroup: 100, // New row group
        }))
        index.incrementalUpdateFromArray(newData, {
          rowGroupsToUpdate: [100],
        })
      })

      bench('incremental update - add 1000 vectors', async () => {
        const newData = generateVectorData(1000, DIMENSIONS).map(d => ({
          ...d,
          rowGroup: 100, // New row group
        }))
        index.incrementalUpdateFromArray(newData, {
          rowGroupsToUpdate: [100],
        })
      })
    })
  })

  // ===========================================================================
  // Memory Statistics
  // ===========================================================================

  describe('Memory and Statistics', () => {
    const DIMENSIONS = 128
    const VECTOR_COUNT = 10000

    let index: VectorIndex
    let storage: MemoryBackend

    beforeAll(async () => {
      storage = new MemoryBackend()
      index = createVectorIndex(storage, DIMENSIONS)
      const data = generateVectorData(VECTOR_COUNT, DIMENSIONS)
      index.buildFromArray(data)
    })

    bench('getStats()', async () => {
      index.getStats()
    })

    bench('getAllDocIds()', async () => {
      index.getAllDocIds()
    })

    bench('hasDocument() - existing', async () => {
      index.hasDocument('vectors/vec-5000')
    })

    bench('hasDocument() - non-existing', async () => {
      index.hasDocument('vectors/vec-nonexistent')
    })

    bench('getRowGroupMetadata()', async () => {
      index.getRowGroupMetadata()
    })

    bench('getDocIdsForRowGroup()', async () => {
      index.getDocIdsForRowGroup(0)
    })
  })
})
