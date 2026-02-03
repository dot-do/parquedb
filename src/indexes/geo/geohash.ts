/**
 * Geohash Encoding/Decoding for ParqueDB Geo Index
 *
 * Standard geohash implementation for spatial indexing.
 * Geohashes divide the Earth into a hierarchical grid using base32 encoding.
 */

// Base32 character set for geohash (lowercase)
const BASE32 = '0123456789bcdefghjkmnpqrstuvwxyz'
const BASE32_MAP = new Map<string, number>()
for (let i = 0; i < BASE32.length; i++) {
  BASE32_MAP.set(BASE32[i]!, i)
}

// Geohash neighbor directions - lookup table for finding the neighbor character
// Each character maps to its neighbor in the specified direction
const NEIGHBORS: Record<string, Record<string, string>> = {
  n: { even: 'p0r21436x8zb9dcf5h7kjnmqesgutwvy', odd: 'bc01fg45238967deuvhjyznpkmstqrwx' },
  s: { even: '14365h7k9dcfesgujnmqp0r2twvyx8zb', odd: '238967debc01fg45uvhjyznpkmstqrwx' },
  e: { even: 'bc01fg45238967deuvhjyznpkmstqrwx', odd: 'p0r21436x8zb9dcf5h7kjnmqesgutwvy' },
  w: { even: '238967debc01fg45kmstqrwxuvhjyznp', odd: '14365h7k9dcfesgujnmqp0r2twvyx8zb' },
}

const BORDERS: Record<string, Record<string, string>> = {
  n: { even: 'prxz', odd: 'bcfguvyz' },
  s: { even: '028b', odd: '0145hjnp' },
  e: { even: 'bcfguvyz', odd: 'prxz' },
  w: { even: '0145hjnp', odd: '028b' },
}

/**
 * Encode latitude/longitude to geohash
 *
 * @param lat - Latitude (-90 to 90)
 * @param lng - Longitude (-180 to 180)
 * @param precision - Number of characters (1-12, default 9 = ~5m precision)
 * @returns Geohash string
 */
export function encodeGeohash(lat: number, lng: number, precision: number = 9): string {
  let minLat = -90
  let maxLat = 90
  let minLng = -180
  let maxLng = 180

  let hash = ''
  let bit = 0
  let ch = 0
  let isLng = true // Start with longitude

  while (hash.length < precision) {
    if (isLng) {
      const mid = (minLng + maxLng) / 2
      if (lng >= mid) {
        ch |= 1 << (4 - bit)
        minLng = mid
      } else {
        maxLng = mid
      }
    } else {
      const mid = (minLat + maxLat) / 2
      if (lat >= mid) {
        ch |= 1 << (4 - bit)
        minLat = mid
      } else {
        maxLat = mid
      }
    }

    isLng = !isLng
    bit++

    if (bit === 5) {
      hash += BASE32[ch]
      bit = 0
      ch = 0
    }
  }

  return hash
}

/**
 * Decoded geohash result with error bounds
 */
export interface GeohashDecodeResult {
  lat: number
  lng: number
  latError: number
  lngError: number
}

/**
 * Decode geohash to latitude/longitude with error bounds
 *
 * @param hash - Geohash string
 * @returns Decoded coordinates with error bounds
 */
export function decodeGeohash(hash: string): GeohashDecodeResult {
  let minLat = -90
  let maxLat = 90
  let minLng = -180
  let maxLng = 180

  let isLng = true

  for (let i = 0; i < hash.length; i++) {
    const char = hash[i]!.toLowerCase()
    const bits = BASE32_MAP.get(char)

    if (bits === undefined) {
      throw new Error(`Invalid geohash character: ${char}`)
    }

    for (let bit = 4; bit >= 0; bit--) {
      const bitValue = (bits >> bit) & 1
      if (isLng) {
        const mid = (minLng + maxLng) / 2
        if (bitValue === 1) {
          minLng = mid
        } else {
          maxLng = mid
        }
      } else {
        const mid = (minLat + maxLat) / 2
        if (bitValue === 1) {
          minLat = mid
        } else {
          maxLat = mid
        }
      }
      isLng = !isLng
    }
  }

  return {
    lat: (minLat + maxLat) / 2,
    lng: (minLng + maxLng) / 2,
    latError: (maxLat - minLat) / 2,
    lngError: (maxLng - minLng) / 2,
  }
}

/**
 * Get bounding box for a geohash
 *
 * @param hash - Geohash string
 * @returns Bounding box [minLat, minLng, maxLat, maxLng]
 */
export function geohashBounds(hash: string): [number, number, number, number] {
  const decoded = decodeGeohash(hash)
  return [
    decoded.lat - decoded.latError,
    decoded.lng - decoded.lngError,
    decoded.lat + decoded.latError,
    decoded.lng + decoded.lngError,
  ]
}

/**
 * Get adjacent geohash in a direction
 *
 * @param hash - Geohash string
 * @param direction - Direction: 'n', 's', 'e', 'w'
 * @returns Adjacent geohash
 */
export function getNeighbor(hash: string, direction: 'n' | 's' | 'e' | 'w'): string {
  if (hash.length === 0) {
    return ''
  }

  hash = hash.toLowerCase()
  const lastChar = hash[hash.length - 1]!
  const type = hash.length % 2 === 0 ? 'even' : 'odd'
  let parent = hash.slice(0, -1)

  // Check if we need to propagate to parent
  if (BORDERS[direction]![type]!.includes(lastChar)) {
    parent = getNeighbor(parent, direction)
    if (parent === '') {
      return '' // Edge of world
    }
  }

  // Get the neighbor character
  const neighborChars = NEIGHBORS[direction]![type]!
  const idx = neighborChars.indexOf(lastChar)
  if (idx === -1) {
    throw new Error(`Invalid geohash character: ${lastChar}`)
  }

  return parent + BASE32[idx]
}

/**
 * Get all 8 neighbors of a geohash cell
 *
 * @param hash - Geohash string
 * @returns Object with neighbors in all 8 directions
 */
export function getNeighbors(hash: string): {
  n: string
  ne: string
  e: string
  se: string
  s: string
  sw: string
  w: string
  nw: string
} {
  const n = getNeighbor(hash, 'n')
  const s = getNeighbor(hash, 's')
  const e = getNeighbor(hash, 'e')
  const w = getNeighbor(hash, 'w')

  return {
    n,
    ne: n ? getNeighbor(n, 'e') : '',
    e,
    se: s ? getNeighbor(s, 'e') : '',
    s,
    sw: s ? getNeighbor(s, 'w') : '',
    w,
    nw: n ? getNeighbor(n, 'w') : '',
  }
}

/**
 * Get all geohash prefixes that overlap with a circle
 *
 * @param lat - Center latitude
 * @param lng - Center longitude
 * @param radiusMeters - Radius in meters
 * @param precision - Optional fixed precision (default: auto-calculated from radius)
 * @returns Set of geohash prefixes that could contain points in the circle
 */
export function geohashesInRadius(
  lat: number,
  lng: number,
  radiusMeters: number,
  precision?: number
): Set<string> {
  // Determine appropriate precision based on radius, or use provided precision
  // Each precision level roughly halves the cell size
  // Precision 1 = ~5000km, 2 = ~1250km, 3 = ~156km, 4 = ~39km, 5 = ~4.9km
  // 6 = ~1.2km, 7 = ~153m, 8 = ~38m, 9 = ~4.8m, 10 = ~1.2m, 11 = ~15cm, 12 = ~2cm
  const effectivePrecision = precision ?? radiusToPrecision(radiusMeters)
  const centerHash = encodeGeohash(lat, lng, effectivePrecision)
  const result = new Set<string>()

  // Add center cell
  result.add(centerHash)

  // Use BFS to expand and add neighboring cells that intersect the circle
  const visited = new Set<string>([centerHash])
  const queue = [centerHash]

  while (queue.length > 0) {
    const current = queue.shift()!
    const neighbors = getNeighbors(current)

    for (const neighbor of Object.values(neighbors)) {
      if (!neighbor || visited.has(neighbor)) {
        continue
      }

      visited.add(neighbor)

      // Check if neighbor cell intersects with circle
      const bounds = geohashBounds(neighbor)
      if (boundsIntersectsCircle(bounds, lat, lng, radiusMeters)) {
        result.add(neighbor)
        queue.push(neighbor)
      }
    }
  }

  return result
}

/**
 * Get optimal geohash precision for a given radius
 */
function radiusToPrecision(radiusMeters: number): number {
  // Cell sizes in meters for each precision level (at equator)
  const cellSizes = [
    5000000, // 1: ~5000km
    1250000, // 2: ~1250km
    156000,  // 3: ~156km
    39000,   // 4: ~39km
    4900,    // 5: ~4.9km
    1200,    // 6: ~1.2km
    153,     // 7: ~153m
    38,      // 8: ~38m
    4.8,     // 9: ~4.8m
    1.2,     // 10: ~1.2m
    0.15,    // 11: ~15cm
    0.019,   // 12: ~2cm
  ]

  // Find the precision where cell size is roughly equal to radius
  // Use a slightly larger cell to ensure coverage
  for (let i = 0; i < cellSizes.length; i++) {
    if (cellSizes[i]! <= radiusMeters * 2) {
      return i + 1
    }
  }

  return 8 // Default to ~38m precision
}

/**
 * Check if a bounding box intersects with a circle
 */
function boundsIntersectsCircle(
  bounds: [number, number, number, number],
  centerLat: number,
  centerLng: number,
  radiusMeters: number
): boolean {
  const [minLat, minLng, maxLat, maxLng] = bounds

  // Find the closest point on the rectangle to the circle center
  const closestLat = Math.max(minLat, Math.min(centerLat, maxLat))
  const closestLng = Math.max(minLng, Math.min(centerLng, maxLng))

  // Calculate distance from circle center to closest point
  // Use simple approximation for speed (exact haversine is in distance.ts)
  const latDiff = (closestLat - centerLat) * 111320 // ~111.32km per degree latitude
  const lngDiff = (closestLng - centerLng) * 111320 * Math.cos(centerLat * Math.PI / 180)
  const distance = Math.sqrt(latDiff * latDiff + lngDiff * lngDiff)

  return distance <= radiusMeters
}

/**
 * Get geohash precision error in meters (approximate)
 *
 * @param precision - Geohash precision (1-12)
 * @returns Error in meters at equator
 */
export function precisionToMeters(precision: number): number {
  // Approximate error bounds at equator for each precision
  const errors = [
    2500000, // 1
    625000,  // 2
    78000,   // 3
    19500,   // 4
    2450,    // 5
    610,     // 6
    76.5,    // 7
    19,      // 8
    2.4,     // 9
    0.6,     // 10
    0.074,   // 11
    0.018,   // 12
  ]
  return errors[Math.max(0, Math.min(11, precision - 1))]!
}
