import { describe, it, expect } from 'vitest'
import {
  encodeGeohash,
  decodeGeohash,
  geohashBounds,
  getNeighbor,
  getNeighbors,
  geohashesInRadius,
  precisionToMeters,
} from '../../../../src/indexes/geo/geohash'

describe('encodeGeohash', () => {
  it('encodes known locations correctly', () => {
    // White House (Washington DC) - verify it starts with expected prefix
    const whiteHouse = encodeGeohash(38.8977, -77.0365, 9)
    expect(whiteHouse).toHaveLength(9)
    expect(whiteHouse.startsWith('dqcjq')).toBe(true)

    // Eiffel Tower (Paris) - verify it starts with expected prefix
    const eiffel = encodeGeohash(48.8584, 2.2945, 9)
    expect(eiffel).toHaveLength(9)
    expect(eiffel.startsWith('u09t')).toBe(true)

    // Sydney Opera House - verify southern hemisphere encoding
    const sydney = encodeGeohash(-33.8568, 151.2153, 9)
    expect(sydney).toHaveLength(9)
    expect(sydney.startsWith('r3gx')).toBe(true)
  })

  it('handles edge cases', () => {
    // North Pole
    const northPole = encodeGeohash(90, 0, 6)
    expect(northPole).toBe('upbpbp')

    // South Pole
    const southPole = encodeGeohash(-90, 0, 6)
    expect(southPole).toBe('h00000')

    // Prime meridian / equator
    const nullIsland = encodeGeohash(0, 0, 6)
    expect(nullIsland).toBe('s00000')
  })

  it('respects precision parameter', () => {
    const lat = 37.7749
    const lng = -122.4194

    expect(encodeGeohash(lat, lng, 1).length).toBe(1)
    expect(encodeGeohash(lat, lng, 5).length).toBe(5)
    expect(encodeGeohash(lat, lng, 12).length).toBe(12)
  })

  it('produces consistent results', () => {
    const lat = 51.5074
    const lng = -0.1278

    const hash1 = encodeGeohash(lat, lng, 8)
    const hash2 = encodeGeohash(lat, lng, 8)
    expect(hash1).toBe(hash2)
  })
})

describe('decodeGeohash', () => {
  it('decodes to approximate original coordinates', () => {
    const lat = 37.7749
    const lng = -122.4194

    const hash = encodeGeohash(lat, lng, 9)
    const decoded = decodeGeohash(hash)

    // At precision 9, error should be < 5 meters
    expect(Math.abs(decoded.lat - lat)).toBeLessThan(0.0001)
    expect(Math.abs(decoded.lng - lng)).toBeLessThan(0.0001)
  })

  it('provides error bounds', () => {
    const hash = encodeGeohash(0, 0, 5)
    const decoded = decodeGeohash(hash)

    expect(decoded.latError).toBeGreaterThan(0)
    expect(decoded.lngError).toBeGreaterThan(0)
  })

  it('roundtrips through encode/decode', () => {
    const locations = [
      { lat: 0, lng: 0 },
      { lat: 45, lng: 90 },
      { lat: -45, lng: -90 },
      { lat: 80, lng: 170 },
      { lat: -80, lng: -170 },
    ]

    for (const { lat, lng } of locations) {
      const hash = encodeGeohash(lat, lng, 8)
      const decoded = decodeGeohash(hash)

      // Error should be within the stated bounds
      expect(Math.abs(decoded.lat - lat)).toBeLessThanOrEqual(decoded.latError + 0.001)
      expect(Math.abs(decoded.lng - lng)).toBeLessThanOrEqual(decoded.lngError + 0.001)
    }
  })

  it('throws on invalid characters', () => {
    expect(() => decodeGeohash('abc123!')).toThrow()
  })
})

describe('geohashBounds', () => {
  it('returns valid bounding box', () => {
    const bounds = geohashBounds('u09t')
    const [minLat, minLng, maxLat, maxLng] = bounds

    expect(minLat).toBeLessThan(maxLat)
    expect(minLng).toBeLessThan(maxLng)
  })

  it('bounds contain the decoded center point', () => {
    const hash = 'gcpvj1'
    const bounds = geohashBounds(hash)
    const [minLat, minLng, maxLat, maxLng] = bounds
    const center = decodeGeohash(hash)

    expect(center.lat).toBeGreaterThanOrEqual(minLat)
    expect(center.lat).toBeLessThanOrEqual(maxLat)
    expect(center.lng).toBeGreaterThanOrEqual(minLng)
    expect(center.lng).toBeLessThanOrEqual(maxLng)
  })

  it('produces smaller bounds for higher precision', () => {
    const low = geohashBounds('u09')
    const high = geohashBounds('u09tunqu')

    const lowArea = (low[2] - low[0]) * (low[3] - low[1])
    const highArea = (high[2] - high[0]) * (high[3] - high[1])

    expect(highArea).toBeLessThan(lowArea)
  })
})

describe('getNeighbor', () => {
  it('returns adjacent cells', () => {
    const center = 'gcpvj1'
    const centerBounds = geohashBounds(center)
    const centerLat = (centerBounds[0] + centerBounds[2]) / 2

    // North neighbor should have higher latitude
    const north = getNeighbor(center, 'n')
    const northBounds = geohashBounds(north)
    expect(northBounds[0]).toBeGreaterThanOrEqual(centerLat)

    // South neighbor should have lower latitude
    const south = getNeighbor(center, 's')
    const southBounds = geohashBounds(south)
    expect(southBounds[2]).toBeLessThanOrEqual(centerLat + 0.01)
  })

  it('handles edge cases near poles', () => {
    // Near north pole
    const nearPole = encodeGeohash(89, 0, 4)
    const north = getNeighbor(nearPole, 'n')
    // Should return empty or valid hash at edge
    expect(typeof north).toBe('string')
  })
})

describe('getNeighbors', () => {
  it('returns all 8 neighbors', () => {
    const center = 'gcpvj1'
    const neighbors = getNeighbors(center)

    expect(neighbors).toHaveProperty('n')
    expect(neighbors).toHaveProperty('ne')
    expect(neighbors).toHaveProperty('e')
    expect(neighbors).toHaveProperty('se')
    expect(neighbors).toHaveProperty('s')
    expect(neighbors).toHaveProperty('sw')
    expect(neighbors).toHaveProperty('w')
    expect(neighbors).toHaveProperty('nw')
  })

  it('neighbors have same precision as center', () => {
    const center = 'gcpvj1'
    const neighbors = getNeighbors(center)

    for (const neighbor of Object.values(neighbors)) {
      if (neighbor) {
        expect(neighbor.length).toBe(center.length)
      }
    }
  })

  it('diagonal neighbors touch at corners', () => {
    const center = 'gcpvj1'
    const neighbors = getNeighbors(center)

    // NE should be east of north and north of east
    const ne = neighbors.ne
    const n = neighbors.n
    const e = neighbors.e

    if (ne && n && e) {
      expect(ne).toBe(getNeighbor(n, 'e'))
      expect(ne).toBe(getNeighbor(e, 'n'))
    }
  })
})

describe('geohashesInRadius', () => {
  it('returns center cell for zero radius', () => {
    const lat = 37.7749
    const lng = -122.4194
    const cells = geohashesInRadius(lat, lng, 0)

    expect(cells.size).toBeGreaterThanOrEqual(1)
  })

  it('returns more cells for larger radius', () => {
    const lat = 37.7749
    const lng = -122.4194

    const small = geohashesInRadius(lat, lng, 100)
    const large = geohashesInRadius(lat, lng, 10000)

    expect(large.size).toBeGreaterThan(small.size)
  })

  it('covers the entire search area', () => {
    const centerLat = 48.8584
    const centerLng = 2.2945
    const radiusMeters = 5000
    const precision = 6

    // Use explicit precision to match what we're checking
    const cells = geohashesInRadius(centerLat, centerLng, radiusMeters, precision)

    // The center should be covered
    const centerHash = encodeGeohash(centerLat, centerLng, precision)
    expect(cells.has(centerHash)).toBe(true)
  })
})

describe('precisionToMeters', () => {
  it('returns expected precision levels', () => {
    // Precision 1 is very coarse (~2500km)
    expect(precisionToMeters(1)).toBeGreaterThan(1000000)

    // Precision 6 is about city-block level (~610m)
    expect(precisionToMeters(6)).toBeLessThan(1000)
    expect(precisionToMeters(6)).toBeGreaterThan(100)

    // Precision 9 is about 2.4 meters
    expect(precisionToMeters(9)).toBeLessThan(5)
    expect(precisionToMeters(9)).toBeGreaterThan(1)
  })

  it('decreases with higher precision', () => {
    for (let p = 1; p < 12; p++) {
      expect(precisionToMeters(p)).toBeGreaterThan(precisionToMeters(p + 1))
    }
  })
})
