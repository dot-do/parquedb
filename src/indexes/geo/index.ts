/**
 * Geo Index Exports for ParqueDB
 *
 * Provides geospatial indexing and proximity search functionality.
 */

export { GeoIndex } from './geo-index'
export type { GeoEntry, GeoSearchResult, GeoSearchOptions } from './geo-index'

export {
  encodeGeohash,
  decodeGeohash,
  geohashBounds,
  getNeighbor,
  getNeighbors,
  geohashesInRadius,
  precisionToMeters,
} from './geohash'
export type { GeohashDecodeResult } from './geohash'

export {
  haversineDistance,
  approximateDistance,
  boundingBox,
  isWithinBoundingBox,
  bearing,
  destination,
  EARTH_RADIUS_METERS,
} from './distance'
export type { BoundingBox } from './distance'
