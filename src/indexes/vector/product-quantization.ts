/**
 * Product Quantization for Vector Index
 *
 * Implements Product Quantization (PQ) to compress vectors while maintaining
 * search quality. This reduces memory usage by 4-8x for high-dimensional vectors.
 *
 * How it works:
 * 1. Split vector into M sub-vectors (sub-quantizers)
 * 2. Train K centroids for each sub-vector using k-means
 * 3. Replace each sub-vector with its nearest centroid index (1 byte)
 * 4. Store only the centroid indices (M bytes per vector vs D*8 bytes)
 *
 * Memory savings example (1024-dim vectors, M=8 sub-quantizers):
 * - Original: 1024 * 8 = 8192 bytes per vector
 * - PQ: 8 bytes per vector
 * - Compression ratio: 1024x
 *
 * Trade-offs:
 * - Search accuracy: ~90-95% recall at typical settings
 * - Build time: Requires training phase
 * - Query time: Slightly slower due to distance table computation
 *
 * References:
 * - Jegou, H., Douze, M., & Schmid, C. (2011). "Product quantization for nearest neighbor search"
 */

import {
  DEFAULT_PQ_SUBQUANTIZERS,
  DEFAULT_PQ_CENTROIDS,
} from '../../constants'

/**
 * Product Quantization codebook
 */
export interface PQCodebook {
  /** Number of sub-quantizers */
  numSubquantizers: number
  /** Number of centroids per sub-quantizer */
  numCentroids: number
  /** Dimension of each sub-vector */
  subvectorDim: number
  /** Original vector dimensions */
  dimensions: number
  /** Centroids: [subquantizer][centroid][subvector_dim] */
  centroids: Float32Array[][]
}

/**
 * Compressed vector using PQ codes
 */
export interface PQCode {
  /** Centroid indices for each sub-quantizer (M bytes) */
  codes: Uint8Array
}

/**
 * Distance table for fast approximate distance computation
 */
export type DistanceTable = Float32Array[]

/**
 * Product Quantization encoder/decoder
 */
export class ProductQuantizer {
  private codebook: PQCodebook | null = null
  private readonly numSubquantizers: number
  private readonly numCentroids: number
  private readonly dimensions: number
  private subvectorDim: number = 0

  constructor(
    dimensions: number,
    numSubquantizers: number = DEFAULT_PQ_SUBQUANTIZERS,
    numCentroids: number = DEFAULT_PQ_CENTROIDS
  ) {
    if (dimensions % numSubquantizers !== 0) {
      // Pad dimensions to be divisible by numSubquantizers
      this.dimensions = Math.ceil(dimensions / numSubquantizers) * numSubquantizers
    } else {
      this.dimensions = dimensions
    }
    this.numSubquantizers = numSubquantizers
    this.numCentroids = numCentroids
    this.subvectorDim = this.dimensions / numSubquantizers
  }

  /**
   * Check if the quantizer has been trained
   */
  get trained(): boolean {
    return this.codebook !== null
  }

  /**
   * Train the product quantizer on a set of vectors
   *
   * @param vectors Training vectors
   * @param options Training options
   */
  train(
    vectors: number[][],
    options?: {
      maxIterations?: number
      epsilon?: number
      sampleSize?: number
    }
  ): void {
    const maxIterations = options?.maxIterations ?? 25
    const epsilon = options?.epsilon ?? 1e-4
    const sampleSize = options?.sampleSize ?? Math.min(vectors.length, 100000)

    // Sample vectors if dataset is too large
    const trainingVectors =
      vectors.length > sampleSize
        ? this.sampleVectors(vectors, sampleSize)
        : vectors

    // Pad vectors if needed
    const paddedVectors = trainingVectors.map(v => this.padVector(v))

    // Initialize codebook
    this.codebook = {
      numSubquantizers: this.numSubquantizers,
      numCentroids: this.numCentroids,
      subvectorDim: this.subvectorDim,
      dimensions: this.dimensions,
      centroids: [],
    }

    // Train each sub-quantizer independently
    for (let m = 0; m < this.numSubquantizers; m++) {
      // Extract sub-vectors for this sub-quantizer
      const subvectors = paddedVectors.map(v =>
        this.extractSubvector(v, m)
      )

      // Run k-means to find centroids
      const centroids = this.kMeans(
        subvectors,
        this.numCentroids,
        maxIterations,
        epsilon
      )

      this.codebook.centroids.push(centroids)
    }
  }

  /**
   * Encode a vector using product quantization
   *
   * @param vector Vector to encode
   * @returns PQ codes
   */
  encode(vector: number[]): PQCode {
    if (!this.codebook) {
      throw new Error('Product quantizer not trained')
    }

    const codes = new Uint8Array(this.numSubquantizers)
    const paddedVector = this.padVector(vector)

    for (let m = 0; m < this.numSubquantizers; m++) {
      const subvector = this.extractSubvector(paddedVector, m)
      codes[m] = this.findNearestCentroid(subvector, m)
    }

    return { codes }
  }

  /**
   * Decode PQ codes back to approximate vector
   *
   * @param code PQ codes
   * @returns Approximate vector
   */
  decode(code: PQCode): number[] {
    if (!this.codebook) {
      throw new Error('Product quantizer not trained')
    }

    const vector: number[] = []

    for (let m = 0; m < this.numSubquantizers; m++) {
      const centroid = this.codebook.centroids[m]![code.codes[m]!]!
      for (let i = 0; i < this.subvectorDim; i++) {
        vector.push(centroid[i]!)
      }
    }

    return vector
  }

  /**
   * Compute distance table for efficient batch distance computation
   *
   * The distance table stores the distance from each sub-vector of the query
   * to each centroid. This allows O(M) distance computation per candidate
   * instead of O(D).
   *
   * @param query Query vector
   * @returns Distance table
   */
  computeDistanceTable(query: number[]): DistanceTable {
    if (!this.codebook) {
      throw new Error('Product quantizer not trained')
    }

    const table: Float32Array[] = []
    const paddedQuery = this.padVector(query)

    for (let m = 0; m < this.numSubquantizers; m++) {
      const subquery = this.extractSubvector(paddedQuery, m)
      const distances = new Float32Array(this.numCentroids)

      for (let k = 0; k < this.numCentroids; k++) {
        distances[k] = this.squaredEuclidean(
          subquery,
          this.codebook.centroids[m]![k]!
        )
      }

      table.push(distances)
    }

    return table
  }

  /**
   * Compute approximate distance using distance table
   *
   * @param table Distance table
   * @param code PQ code
   * @returns Approximate squared Euclidean distance
   */
  computeDistance(table: DistanceTable, code: PQCode): number {
    let distance = 0

    for (let m = 0; m < this.numSubquantizers; m++) {
      distance += table[m]![code.codes[m]!]!
    }

    return distance
  }

  /**
   * Get memory size of encoded vector in bytes
   */
  get codeSize(): number {
    return this.numSubquantizers
  }

  /**
   * Get memory size of codebook in bytes
   */
  get codebookSize(): number {
    // centroids: numSubquantizers * numCentroids * subvectorDim * 4 bytes
    return (
      this.numSubquantizers * this.numCentroids * this.subvectorDim * 4
    )
  }

  /**
   * Serialize the codebook
   */
  serialize(): Uint8Array {
    if (!this.codebook) {
      throw new Error('Product quantizer not trained')
    }

    // Calculate size:
    // 4 bytes: numSubquantizers
    // 4 bytes: numCentroids
    // 4 bytes: subvectorDim
    // 4 bytes: dimensions
    // centroids: numSubquantizers * numCentroids * subvectorDim * 4 bytes
    const headerSize = 16
    const centroidsSize =
      this.numSubquantizers * this.numCentroids * this.subvectorDim * 4

    const buffer = new ArrayBuffer(headerSize + centroidsSize)
    const view = new DataView(buffer)
    let offset = 0

    // Header
    view.setUint32(offset, this.numSubquantizers, false)
    offset += 4
    view.setUint32(offset, this.numCentroids, false)
    offset += 4
    view.setUint32(offset, this.subvectorDim, false)
    offset += 4
    view.setUint32(offset, this.dimensions, false)
    offset += 4

    // Centroids
    for (let m = 0; m < this.numSubquantizers; m++) {
      for (let k = 0; k < this.numCentroids; k++) {
        const centroid = this.codebook.centroids[m]![k]!
        for (let d = 0; d < this.subvectorDim; d++) {
          view.setFloat32(offset, centroid[d]!, false)
          offset += 4
        }
      }
    }

    return new Uint8Array(buffer)
  }

  /**
   * Deserialize a codebook
   */
  deserialize(data: Uint8Array): void {
    const view = new DataView(data.buffer, data.byteOffset, data.byteLength)
    let offset = 0

    const numSubquantizers = view.getUint32(offset, false)
    offset += 4
    const numCentroids = view.getUint32(offset, false)
    offset += 4
    const subvectorDim = view.getUint32(offset, false)
    offset += 4
    const dimensions = view.getUint32(offset, false)
    offset += 4

    this.subvectorDim = subvectorDim

    const centroids: Float32Array[][] = []

    for (let m = 0; m < numSubquantizers; m++) {
      const subCentroids: Float32Array[] = []
      for (let k = 0; k < numCentroids; k++) {
        const centroid = new Float32Array(subvectorDim)
        for (let d = 0; d < subvectorDim; d++) {
          centroid[d] = view.getFloat32(offset, false)
          offset += 4
        }
        subCentroids.push(centroid)
      }
      centroids.push(subCentroids)
    }

    this.codebook = {
      numSubquantizers,
      numCentroids,
      subvectorDim,
      dimensions,
      centroids,
    }
  }

  // ===========================================================================
  // Private Helpers
  // ===========================================================================

  private padVector(vector: number[]): number[] {
    if (vector.length >= this.dimensions) {
      return vector.slice(0, this.dimensions)
    }
    const padded = new Array(this.dimensions).fill(0)
    for (let i = 0; i < vector.length; i++) {
      padded[i] = vector[i]!
    }
    return padded
  }

  private extractSubvector(vector: number[], subquantizerIndex: number): Float32Array {
    const start = subquantizerIndex * this.subvectorDim
    const subvector = new Float32Array(this.subvectorDim)
    for (let i = 0; i < this.subvectorDim; i++) {
      subvector[i] = vector[start + i]!
    }
    return subvector
  }

  private findNearestCentroid(subvector: Float32Array, subquantizerIndex: number): number {
    if (!this.codebook) {
      throw new Error('Product quantizer not trained')
    }

    let nearestIndex = 0
    let nearestDistance = Infinity

    for (let k = 0; k < this.numCentroids; k++) {
      const centroid = this.codebook.centroids[subquantizerIndex]![k]!
      const distance = this.squaredEuclidean(subvector, centroid)

      if (distance < nearestDistance) {
        nearestDistance = distance
        nearestIndex = k
      }
    }

    return nearestIndex
  }

  private squaredEuclidean(a: Float32Array, b: Float32Array): number {
    let sum = 0
    for (let i = 0; i < a.length; i++) {
      const diff = a[i]! - b[i]!
      sum += diff * diff
    }
    return sum
  }

  private sampleVectors(vectors: number[][], sampleSize: number): number[][] {
    // Fisher-Yates shuffle for first sampleSize elements
    const indices = Array.from({ length: vectors.length }, (_, i) => i)

    for (let i = 0; i < sampleSize && i < vectors.length; i++) {
      const j = i + Math.floor(Math.random() * (vectors.length - i))
      const temp = indices[i]!
      indices[i] = indices[j]!
      indices[j] = temp
    }

    return indices.slice(0, sampleSize).map(i => vectors[i]!)
  }

  private kMeans(
    vectors: Float32Array[],
    k: number,
    maxIterations: number,
    epsilon: number
  ): Float32Array[] {
    if (vectors.length === 0) {
      // Return random centroids if no training data
      return Array.from({ length: k }, () =>
        Float32Array.from({ length: this.subvectorDim }, () => Math.random())
      )
    }

    // Initialize centroids using k-means++
    const centroids = this.kMeansPlusPlusInit(vectors, k)

    for (let iteration = 0; iteration < maxIterations; iteration++) {
      // Assign points to nearest centroid
      const assignments: number[][] = Array.from({ length: k }, () => [])

      for (let i = 0; i < vectors.length; i++) {
        let nearestIdx = 0
        let nearestDist = Infinity

        for (let j = 0; j < k; j++) {
          const dist = this.squaredEuclidean(vectors[i]!, centroids[j]!)
          if (dist < nearestDist) {
            nearestDist = dist
            nearestIdx = j
          }
        }

        assignments[nearestIdx]!.push(i)
      }

      // Update centroids
      let maxChange = 0

      for (let j = 0; j < k; j++) {
        if (assignments[j]!.length === 0) continue

        const newCentroid = new Float32Array(this.subvectorDim)

        for (const idx of assignments[j]!) {
          for (let d = 0; d < this.subvectorDim; d++) {
            newCentroid[d]! += vectors[idx]![d]!
          }
        }

        for (let d = 0; d < this.subvectorDim; d++) {
          newCentroid[d]! /= assignments[j]!.length
        }

        const change = this.squaredEuclidean(centroids[j]!, newCentroid)
        maxChange = Math.max(maxChange, change)
        centroids[j] = newCentroid
      }

      // Check convergence
      if (maxChange < epsilon) break
    }

    return centroids
  }

  private kMeansPlusPlusInit(vectors: Float32Array[], k: number): Float32Array[] {
    const centroids: Float32Array[] = []

    // First centroid: random
    const firstIdx = Math.floor(Math.random() * vectors.length)
    centroids.push(Float32Array.from(vectors[firstIdx]!))

    // Remaining centroids: weighted by distance to nearest centroid
    for (let i = 1; i < k; i++) {
      const distances: number[] = []
      let totalDistance = 0

      for (const vector of vectors) {
        let minDist = Infinity
        for (const centroid of centroids) {
          const dist = this.squaredEuclidean(vector, centroid)
          minDist = Math.min(minDist, dist)
        }
        distances.push(minDist)
        totalDistance += minDist
      }

      // Sample proportional to squared distance
      let threshold = Math.random() * totalDistance
      let selectedIdx = 0

      for (let j = 0; j < distances.length; j++) {
        threshold -= distances[j]!
        if (threshold <= 0) {
          selectedIdx = j
          break
        }
      }

      centroids.push(Float32Array.from(vectors[selectedIdx]!))
    }

    return centroids
  }
}
