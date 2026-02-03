import { describe, it, expect } from 'vitest'
import {
  haversineDistance,
  approximateDistance,
  boundingBox,
  isWithinBoundingBox,
  bearing,
  destination,
  EARTH_RADIUS_METERS,
} from '../../../../src/indexes/geo/distance'

describe('haversineDistance', () => {
  it('returns zero for same point', () => {
    const distance = haversineDistance(37.7749, -122.4194, 37.7749, -122.4194)
    expect(distance).toBe(0)
  })

  it('calculates SF to LA correctly (~559km)', () => {
    // San Francisco
    const sfLat = 37.7749
    const sfLng = -122.4194

    // Los Angeles
    const laLat = 34.0522
    const laLng = -118.2437

    const distance = haversineDistance(sfLat, sfLng, laLat, laLng)

    // Expected: approximately 559km
    expect(distance).toBeGreaterThan(550000)
    expect(distance).toBeLessThan(570000)
  })

  it('calculates London to Paris correctly (~344km)', () => {
    // London
    const lonLat = 51.5074
    const lonLng = -0.1278

    // Paris
    const parLat = 48.8566
    const parLng = 2.3522

    const distance = haversineDistance(lonLat, lonLng, parLat, parLng)

    // Expected: approximately 344km
    expect(distance).toBeGreaterThan(340000)
    expect(distance).toBeLessThan(350000)
  })

  it('calculates NYC to Sydney correctly (~15989km)', () => {
    // New York
    const nycLat = 40.7128
    const nycLng = -74.0060

    // Sydney
    const sydLat = -33.8688
    const sydLng = 151.2093

    const distance = haversineDistance(nycLat, nycLng, sydLat, sydLng)

    // Expected: approximately 15989km
    expect(distance).toBeGreaterThan(15800000)
    expect(distance).toBeLessThan(16200000)
  })

  it('is symmetric', () => {
    const d1 = haversineDistance(37.7749, -122.4194, 34.0522, -118.2437)
    const d2 = haversineDistance(34.0522, -118.2437, 37.7749, -122.4194)

    expect(d1).toBeCloseTo(d2, 5)
  })

  it('handles antipodal points', () => {
    // Distance should be approximately half the Earth's circumference
    const distance = haversineDistance(0, 0, 0, 180)
    const halfCircumference = Math.PI * EARTH_RADIUS_METERS

    expect(distance).toBeCloseTo(halfCircumference, -4)
  })

  it('handles crossing the antimeridian', () => {
    // Tokyo to San Francisco crossing Pacific
    const distance = haversineDistance(35.6762, 139.6503, 37.7749, -122.4194)

    // Should be approximately 8280km
    expect(distance).toBeGreaterThan(8200000)
    expect(distance).toBeLessThan(8400000)
  })
})

describe('approximateDistance', () => {
  it('is close to haversine for short distances', () => {
    // Two points about 10km apart
    const lat1 = 37.7749
    const lng1 = -122.4194
    const lat2 = 37.8549
    const lng2 = -122.4794

    const exact = haversineDistance(lat1, lng1, lat2, lng2)
    const approx = approximateDistance(lat1, lng1, lat2, lng2)

    // Should be within 5% for short distances
    expect(Math.abs(exact - approx) / exact).toBeLessThan(0.05)
  })

  it('is faster for filtering (sanity check)', () => {
    // Just ensure it runs and returns reasonable values
    const distance = approximateDistance(0, 0, 1, 1)
    expect(distance).toBeGreaterThan(100000)
    expect(distance).toBeLessThan(200000)
  })
})

describe('boundingBox', () => {
  it('creates valid bounding box', () => {
    const box = boundingBox(0, 0, 1000)

    expect(box.minLat).toBeLessThan(box.maxLat)
    expect(box.minLng).toBeLessThan(box.maxLng)
  })

  it('contains the center point', () => {
    const lat = 37.7749
    const lng = -122.4194
    const box = boundingBox(lat, lng, 5000)

    expect(lat).toBeGreaterThanOrEqual(box.minLat)
    expect(lat).toBeLessThanOrEqual(box.maxLat)
    expect(lng).toBeGreaterThanOrEqual(box.minLng)
    expect(lng).toBeLessThanOrEqual(box.maxLng)
  })

  it('size increases with radius', () => {
    const lat = 45
    const lng = 90

    const small = boundingBox(lat, lng, 1000)
    const large = boundingBox(lat, lng, 10000)

    const smallArea = (small.maxLat - small.minLat) * (small.maxLng - small.minLng)
    const largeArea = (large.maxLat - large.minLat) * (large.maxLng - large.minLng)

    expect(largeArea).toBeGreaterThan(smallArea)
  })

  it('handles near-pole latitudes', () => {
    // Near north pole
    const box = boundingBox(89, 0, 100000)

    expect(box.minLat).toBeLessThanOrEqual(90)
    expect(box.maxLat).toBeLessThanOrEqual(90)
    expect(box.minLng).toBeGreaterThanOrEqual(-180)
    expect(box.maxLng).toBeLessThanOrEqual(180)
  })

  it('handles equator correctly', () => {
    const box = boundingBox(0, 0, 1000)

    // At equator, lat and lng extents should be similar
    const latExtent = box.maxLat - box.minLat
    const lngExtent = box.maxLng - box.minLng

    expect(Math.abs(latExtent - lngExtent)).toBeLessThan(0.01)
  })
})

describe('isWithinBoundingBox', () => {
  it('returns true for center point', () => {
    const box = boundingBox(37.7749, -122.4194, 1000)
    expect(isWithinBoundingBox(37.7749, -122.4194, box)).toBe(true)
  })

  it('returns false for distant point', () => {
    const box = boundingBox(37.7749, -122.4194, 1000)
    expect(isWithinBoundingBox(0, 0, box)).toBe(false)
  })

  it('returns true for points on edges', () => {
    const box = boundingBox(0, 0, 1000)
    expect(isWithinBoundingBox(box.minLat, 0, box)).toBe(true)
    expect(isWithinBoundingBox(box.maxLat, 0, box)).toBe(true)
    expect(isWithinBoundingBox(0, box.minLng, box)).toBe(true)
    expect(isWithinBoundingBox(0, box.maxLng, box)).toBe(true)
  })
})

describe('bearing', () => {
  it('returns 0 for due north', () => {
    const b = bearing(0, 0, 1, 0)
    expect(b).toBeCloseTo(0, 0)
  })

  it('returns 90 for due east', () => {
    const b = bearing(0, 0, 0, 1)
    expect(b).toBeCloseTo(90, 0)
  })

  it('returns 180 for due south', () => {
    const b = bearing(0, 0, -1, 0)
    expect(b).toBeCloseTo(180, 0)
  })

  it('returns 270 for due west', () => {
    const b = bearing(0, 0, 0, -1)
    expect(b).toBeCloseTo(270, 0)
  })

  it('calculates diagonal correctly', () => {
    // Northeast should be around 45
    const ne = bearing(0, 0, 1, 1)
    expect(ne).toBeGreaterThan(40)
    expect(ne).toBeLessThan(50)
  })
})

describe('destination', () => {
  it('going north increases latitude', () => {
    const [lat, lng] = destination(0, 0, 0, 10000)
    expect(lat).toBeGreaterThan(0)
    expect(Math.abs(lng)).toBeLessThan(0.001)
  })

  it('going east increases longitude', () => {
    const [lat, lng] = destination(0, 0, 90, 10000)
    expect(Math.abs(lat)).toBeLessThan(0.001)
    expect(lng).toBeGreaterThan(0)
  })

  it('going south decreases latitude', () => {
    const [lat, lng] = destination(0, 0, 180, 10000)
    expect(lat).toBeLessThan(0)
    expect(Math.abs(lng)).toBeLessThan(0.001)
  })

  it('roundtrips with bearing', () => {
    const startLat = 37.7749
    const startLng = -122.4194
    const distance = 5000
    const bearingDeg = 45

    const [endLat, endLng] = destination(startLat, startLng, bearingDeg, distance)

    // Calculate distance back should equal original
    const calcDistance = haversineDistance(startLat, startLng, endLat, endLng)
    expect(calcDistance).toBeCloseTo(distance, -1)

    // Bearing to destination should match
    const calcBearing = bearing(startLat, startLng, endLat, endLng)
    expect(calcBearing).toBeCloseTo(bearingDeg, 0)
  })
})
