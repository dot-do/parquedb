/**
 * Distance Functions for Geo Index
 *
 * Haversine distance and bounding box calculations for spatial queries.
 */

/**
 * Earth radius in meters
 */
export const EARTH_RADIUS_METERS = 6371008.8

/**
 * Haversine distance between two points on Earth
 *
 * Uses the haversine formula for great-circle distance.
 * Accurate for most distances, slight error for antipodal points.
 *
 * @param lat1 - Latitude of first point in degrees
 * @param lng1 - Longitude of first point in degrees
 * @param lat2 - Latitude of second point in degrees
 * @param lng2 - Longitude of second point in degrees
 * @returns Distance in meters
 */
export function haversineDistance(
  lat1: number,
  lng1: number,
  lat2: number,
  lng2: number
): number {
  const toRadians = (deg: number) => deg * (Math.PI / 180)

  const dLat = toRadians(lat2 - lat1)
  const dLng = toRadians(lng2 - lng1)
  const lat1Rad = toRadians(lat1)
  const lat2Rad = toRadians(lat2)

  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(lat1Rad) * Math.cos(lat2Rad) * Math.sin(dLng / 2) * Math.sin(dLng / 2)

  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))

  return EARTH_RADIUS_METERS * c
}

/**
 * Fast approximate distance for initial filtering
 *
 * Uses equirectangular approximation. Good for short distances
 * and filtering before exact haversine calculation.
 *
 * @param lat1 - Latitude of first point in degrees
 * @param lng1 - Longitude of first point in degrees
 * @param lat2 - Latitude of second point in degrees
 * @param lng2 - Longitude of second point in degrees
 * @returns Approximate distance in meters
 */
export function approximateDistance(
  lat1: number,
  lng1: number,
  lat2: number,
  lng2: number
): number {
  const toRadians = (deg: number) => deg * (Math.PI / 180)

  const avgLat = (lat1 + lat2) / 2
  const x = toRadians(lng2 - lng1) * Math.cos(toRadians(avgLat))
  const y = toRadians(lat2 - lat1)

  return Math.sqrt(x * x + y * y) * EARTH_RADIUS_METERS
}

/**
 * Bounding box result
 */
export interface BoundingBox {
  minLat: number
  maxLat: number
  minLng: number
  maxLng: number
}

/**
 * Calculate bounding box for a point with radius
 *
 * @param lat - Center latitude in degrees
 * @param lng - Center longitude in degrees
 * @param radiusMeters - Radius in meters
 * @returns Bounding box
 */
export function boundingBox(lat: number, lng: number, radiusMeters: number): BoundingBox {
  // Angular distance in radians
  const angularDistance = radiusMeters / EARTH_RADIUS_METERS

  const latRad = lat * (Math.PI / 180)
  const lngRad = lng * (Math.PI / 180)

  // Latitude bounds
  const minLatRad = latRad - angularDistance
  const maxLatRad = latRad + angularDistance

  // Longitude bounds (accounting for latitude compression)
  let minLngRad: number
  let maxLngRad: number

  // Check if we're at the poles
  if (minLatRad > -Math.PI / 2 && maxLatRad < Math.PI / 2) {
    // Normal case - not crossing poles
    const deltaLng = Math.asin(Math.sin(angularDistance) / Math.cos(latRad))
    minLngRad = lngRad - deltaLng
    maxLngRad = lngRad + deltaLng

    // Handle crossing the antimeridian
    if (minLngRad < -Math.PI) {
      minLngRad += 2 * Math.PI
    }
    if (maxLngRad > Math.PI) {
      maxLngRad -= 2 * Math.PI
    }
  } else {
    // Crossing a pole - full longitude range
    minLngRad = -Math.PI
    maxLngRad = Math.PI
  }

  return {
    minLat: Math.max(-90, minLatRad * (180 / Math.PI)),
    maxLat: Math.min(90, maxLatRad * (180 / Math.PI)),
    minLng: minLngRad * (180 / Math.PI),
    maxLng: maxLngRad * (180 / Math.PI),
  }
}

/**
 * Check if a point is within a bounding box
 *
 * @param lat - Point latitude
 * @param lng - Point longitude
 * @param box - Bounding box
 * @returns True if point is within box
 */
export function isWithinBoundingBox(lat: number, lng: number, box: BoundingBox): boolean {
  if (lat < box.minLat || lat > box.maxLat) {
    return false
  }

  // Handle antimeridian crossing (minLng > maxLng)
  if (box.minLng > box.maxLng) {
    return lng >= box.minLng || lng <= box.maxLng
  }

  return lng >= box.minLng && lng <= box.maxLng
}

/**
 * Calculate the bearing from one point to another
 *
 * @param lat1 - Latitude of first point in degrees
 * @param lng1 - Longitude of first point in degrees
 * @param lat2 - Latitude of second point in degrees
 * @param lng2 - Longitude of second point in degrees
 * @returns Bearing in degrees (0-360, clockwise from north)
 */
export function bearing(lat1: number, lng1: number, lat2: number, lng2: number): number {
  const toRadians = (deg: number) => deg * (Math.PI / 180)
  const toDegrees = (rad: number) => rad * (180 / Math.PI)

  const dLng = toRadians(lng2 - lng1)
  const lat1Rad = toRadians(lat1)
  const lat2Rad = toRadians(lat2)

  const y = Math.sin(dLng) * Math.cos(lat2Rad)
  const x =
    Math.cos(lat1Rad) * Math.sin(lat2Rad) -
    Math.sin(lat1Rad) * Math.cos(lat2Rad) * Math.cos(dLng)

  const bearingRad = Math.atan2(y, x)
  const bearingDeg = toDegrees(bearingRad)

  // Normalize to 0-360
  return (bearingDeg + 360) % 360
}

/**
 * Calculate destination point given start, bearing, and distance
 *
 * @param lat - Start latitude in degrees
 * @param lng - Start longitude in degrees
 * @param bearingDeg - Bearing in degrees (clockwise from north)
 * @param distanceMeters - Distance in meters
 * @returns Destination point [lat, lng]
 */
export function destination(
  lat: number,
  lng: number,
  bearingDeg: number,
  distanceMeters: number
): [number, number] {
  const toRadians = (deg: number) => deg * (Math.PI / 180)
  const toDegrees = (rad: number) => rad * (180 / Math.PI)

  const latRad = toRadians(lat)
  const lngRad = toRadians(lng)
  const bearingRad = toRadians(bearingDeg)
  const angularDistance = distanceMeters / EARTH_RADIUS_METERS

  const sinLat1 = Math.sin(latRad)
  const cosLat1 = Math.cos(latRad)
  const sinAngDist = Math.sin(angularDistance)
  const cosAngDist = Math.cos(angularDistance)

  const lat2Rad = Math.asin(
    sinLat1 * cosAngDist + cosLat1 * sinAngDist * Math.cos(bearingRad)
  )
  const lng2Rad =
    lngRad +
    Math.atan2(
      Math.sin(bearingRad) * sinAngDist * cosLat1,
      cosAngDist - sinLat1 * Math.sin(lat2Rad)
    )

  return [toDegrees(lat2Rad), toDegrees(lng2Rad)]
}
