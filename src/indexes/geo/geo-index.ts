/**
 * GeoIndex for ParqueDB
 *
 * Spatial index using geohash bucketing for efficient proximity queries.
 * Supports $near queries with distance filtering.
 */

import type { StorageBackend } from '../../types/storage'
import type { IndexDefinition, IndexStats } from '../types'
import { encodeGeohash, geohashesInRadius, decodeGeohash as _decodeGeohash } from './geohash'
import { haversineDistance, boundingBox, isWithinBoundingBox } from './distance'
import { logger } from '../../utils/logger'

/**
 * Entry in the geo index
 */
export interface GeoEntry {
  /** Document ID */
  docId: string
  /** Latitude */
  lat: number
  /** Longitude */
  lng: number
  /** Row group number */
  rowGroup: number
  /** Row offset within row group */
  rowOffset: number
  /** Precomputed geohash for fast filtering */
  geohash: string
}

/**
 * Search result from geo index
 */
export interface GeoSearchResult {
  /** Matching document IDs (ordered by distance) */
  docIds: string[]
  /** Row group hints for efficient reading */
  rowGroups: number[]
  /** Distances in meters for each result */
  distances: number[]
  /** Number of entries scanned */
  entriesScanned: number
}

/**
 * Geo search options
 */
export interface GeoSearchOptions {
  /** Maximum distance in meters */
  maxDistance?: number | undefined
  /** Minimum distance in meters */
  minDistance?: number | undefined
  /** Maximum results to return */
  limit?: number | undefined
}

/**
 * Serialized geo index format
 */
interface SerializedGeoIndex {
  version: number
  entries: Array<{
    docId: string
    lat: number
    lng: number
    rowGroup: number
    rowOffset: number
    geohash: string
  }>
  buckets: Record<string, string[]> // geohash prefix -> docIds
}

/**
 * GeoIndex implementation using geohash bucketing
 *
 * Uses geohash prefixes to bucket entries for O(1) candidate lookup.
 * Similar to an inverted index where the "terms" are geohash cells.
 */
export class GeoIndex {
  /** Geohash bucket precision for indexing */
  private static readonly BUCKET_PRECISION = 6 // ~1.2km cells

  /** All entries by docId */
  private entries: Map<string, GeoEntry> = new Map()

  /** Geohash buckets: prefix -> docIds */
  private buckets: Map<string, Set<string>> = new Map()

  /** Whether index is loaded */
  private loaded: boolean = false

  constructor(
    private storage: StorageBackend,
    private namespace: string,
    private definition: IndexDefinition,
    private basePath: string = ''
  ) {}

  /**
   * Check if index is ready
   */
  get ready(): boolean {
    return this.loaded
  }

  /**
   * Insert a point into the index
   */
  insert(
    docId: string,
    lat: number,
    lng: number,
    rowGroup: number,
    rowOffset: number
  ): void {
    // Remove existing entry if present
    if (this.entries.has(docId)) {
      this.remove(docId)
    }

    // Compute geohash at bucket precision
    const geohash = encodeGeohash(lat, lng, GeoIndex.BUCKET_PRECISION)

    // Create entry
    const entry: GeoEntry = {
      docId,
      lat,
      lng,
      rowGroup,
      rowOffset,
      geohash,
    }

    // Store in entries map
    this.entries.set(docId, entry)

    // Add to bucket
    if (!this.buckets.has(geohash)) {
      this.buckets.set(geohash, new Set())
    }
    this.buckets.get(geohash)!.add(docId)
  }

  /**
   * Remove a document from the index
   */
  remove(docId: string): boolean {
    const entry = this.entries.get(docId)
    if (!entry) {
      return false
    }

    // Remove from bucket
    const bucket = this.buckets.get(entry.geohash)
    if (bucket) {
      bucket.delete(docId)
      if (bucket.size === 0) {
        this.buckets.delete(entry.geohash)
      }
    }

    // Remove from entries
    this.entries.delete(docId)
    return true
  }

  /**
   * Search for points near a location
   */
  search(
    centerLat: number,
    centerLng: number,
    options: GeoSearchOptions = {}
  ): GeoSearchResult {
    const { maxDistance = Infinity, minDistance = 0, limit = 100 } = options

    // Get candidate geohash cells that might contain results
    // Use bucket precision to match how entries are stored
    const candidateCells = geohashesInRadius(centerLat, centerLng, maxDistance, GeoIndex.BUCKET_PRECISION)

    // Collect all candidates from matching buckets
    const candidates: GeoEntry[] = []
    let entriesScanned = 0

    for (const cell of candidateCells) {
      const bucket = this.buckets.get(cell)
      if (bucket) {
        for (const docId of bucket) {
          const entry = this.entries.get(docId)
          if (entry) {
            candidates.push(entry)
            entriesScanned++
          }
        }
      }
    }

    // Early exit if no candidates
    if (candidates.length === 0) {
      return {
        docIds: [],
        rowGroups: [],
        distances: [],
        entriesScanned: 0,
      }
    }

    // Fast bounding box filter first
    const bbox = boundingBox(centerLat, centerLng, maxDistance)
    const inBbox = candidates.filter(entry =>
      isWithinBoundingBox(entry.lat, entry.lng, bbox)
    )

    // Calculate exact distances and filter
    const withDistances: Array<{ entry: GeoEntry; distance: number }> = []

    for (const entry of inBbox) {
      const distance = haversineDistance(centerLat, centerLng, entry.lat, entry.lng)

      if (distance >= minDistance && distance <= maxDistance) {
        withDistances.push({ entry, distance })
      }
    }

    // Sort by distance
    withDistances.sort((a, b) => a.distance - b.distance)

    // Apply limit
    const limited = withDistances.slice(0, limit)

    return {
      docIds: limited.map(r => r.entry.docId),
      rowGroups: limited.map(r => r.entry.rowGroup),
      distances: limited.map(r => r.distance),
      entriesScanned,
    }
  }

  /**
   * Get all document IDs in the index
   */
  getAllDocIds(): Set<string> {
    return new Set(this.entries.keys())
  }

  /**
   * Get entry by document ID
   */
  getEntry(docId: string): GeoEntry | undefined {
    return this.entries.get(docId)
  }

  /**
   * Get index statistics
   */
  getStats(): IndexStats {
    return {
      entryCount: this.entries.size,
      sizeBytes: this.estimateSize(),
      uniqueKeys: this.buckets.size,
    }
  }

  /**
   * Load the index from storage
   */
  async load(): Promise<void> {
    if (this.loaded) return

    const indexPath = this.getIndexPath()
    const exists = await this.storage.exists(indexPath)

    if (!exists) {
      this.loaded = true
      return
    }

    try {
      const data = await this.storage.read(indexPath)
      const json = JSON.parse(new TextDecoder().decode(data)) as SerializedGeoIndex

      if (json.version !== 1) {
        throw new Error(`Unsupported geo index version: ${json.version}`)
      }

      // Load entries
      for (const entry of json.entries) {
        this.entries.set(entry.docId, entry)
      }

      // Rebuild buckets
      for (const [prefix, docIds] of Object.entries(json.buckets)) {
        this.buckets.set(prefix, new Set(docIds))
      }

      this.loaded = true
    } catch (error) {
      // Log and continue with empty index
      logger.warn('Failed to load geo index, starting fresh:', error)
      this.entries.clear()
      this.buckets.clear()
      this.loaded = true
    }
  }

  /**
   * Save the index to storage
   */
  async save(): Promise<void> {
    const indexPath = this.getIndexPath()

    // Serialize
    const serialized: SerializedGeoIndex = {
      version: 1,
      entries: Array.from(this.entries.values()),
      buckets: Object.fromEntries(
        Array.from(this.buckets.entries()).map(([k, v]) => [k, Array.from(v)])
      ),
    }

    const data = new TextEncoder().encode(JSON.stringify(serialized))

    // Ensure directory exists
    const dir = indexPath.substring(0, indexPath.lastIndexOf('/'))
    await this.ensureDirectory(dir)

    await this.storage.write(indexPath, data)
  }

  /**
   * Clear all entries
   */
  clear(): void {
    this.entries.clear()
    this.buckets.clear()
  }

  /**
   * Get index file path
   */
  private getIndexPath(): string {
    const base = this.basePath ? `${this.basePath}/` : ''
    return `${base}indexes/geo/${this.namespace}.${this.definition.name}.geoidx`
  }

  /**
   * Ensure a directory exists
   */
  private async ensureDirectory(_path: string): Promise<void> {
    // Most storage backends create directories implicitly on write
    // This is a no-op for now but can be extended
  }

  /**
   * Estimate memory size of the index
   */
  private estimateSize(): number {
    // Rough estimate: 100 bytes per entry
    return this.entries.size * 100
  }
}
